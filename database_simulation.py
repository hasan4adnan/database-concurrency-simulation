#!/usr/bin/env python3
"""Concurrent transaction simulation for AdventureWorks on SQL Server.

This version keeps the original project structure but hardens the simulation so
it always finishes, even when SQL Server lock waits become severe. The main
changes are:

1. Every connection and cursor uses a timeout.
2. Every transaction sets a SQL Server lock timeout.
3. Worker threads are joined with a timeout instead of waiting forever.
4. Worker threads are daemon threads, so a stuck driver call cannot block
   process shutdown.
5. Deadlocks and lock timeouts are handled gracefully and counted/reported.
"""

import argparse
import random
import statistics
import threading
import time

import pyodbc


DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_SERVER = "localhost,1433"
DEFAULT_DATABASE = "AdventureWorks"
DEFAULT_USERNAME = "sa"
DEFAULT_PASSWORD = "YourStrong!Pass123"
TRANSACTIONS_PER_THREAD = 5

# Timeouts are intentionally conservative so the simulation remains responsive
# even under REPEATABLE READ and SERIALIZABLE.
DEFAULT_CONNECTION_TIMEOUT_SECONDS = 5
DEFAULT_QUERY_TIMEOUT_SECONDS = 5
DEFAULT_LOCK_TIMEOUT_MILLISECONDS = 5000
DEFAULT_JOIN_TIMEOUT_SECONDS = 60

ISOLATION_LEVELS = {
    "READ UNCOMMITTED": "READ UNCOMMITTED",
    "READ COMMITTED": "READ COMMITTED",
    "REPEATABLE READ": "REPEATABLE READ",
    "SERIALIZABLE": "SERIALIZABLE",
}

# The current workload uses lightweight SQL to avoid heavy contention while
# still exercising concurrent transactions. READPAST is only valid for
# READ COMMITTED and REPEATABLE READ, so the final SQL is built per isolation
# level instead of using one static query string for all runs.
TYPE_A_QUERY_TEMPLATE = """
UPDATE TOP (1) Sales.SalesOrderDetail {table_hint}
SET UnitPrice = UnitPrice
WHERE SalesOrderID = ABS(CHECKSUM(NEWID())) % 50000
"""

TYPE_B_QUERY_TEMPLATE = """
SELECT TOP (1) OrderQty
FROM Sales.SalesOrderDetail {table_hint}
ORDER BY NEWID()
"""


def build_connection_string(args):
    """Create a SQL Server connection string for pyodbc."""
    return (
        f"DRIVER={{{args.driver}}};"
        f"SERVER={args.server};"
        f"DATABASE={args.database};"
        f"UID={args.username};"
        f"PWD={args.password};"
        "TrustServerCertificate=yes;"
    )


def normalize_isolation_level(level_text):
    """Convert user input into one of the supported isolation level labels."""
    normalized = " ".join(level_text.replace("_", " ").replace("-", " ").upper().split())
    if normalized not in ISOLATION_LEVELS:
        raise ValueError(
            f"Unsupported isolation level '{level_text}'. "
            f"Choose from: {', '.join(ISOLATION_LEVELS)}"
        )
    return normalized


def uses_readpast(isolation_level):
    """Return True when READPAST is valid for the given isolation level."""
    return isolation_level in {"READ COMMITTED", "REPEATABLE READ"}


def build_type_a_query(isolation_level):
    """Build the Type A UPDATE query for the selected isolation level."""
    if uses_readpast(isolation_level):
        table_hint = "WITH (ROWLOCK, READPAST)"
    else:
        table_hint = "WITH (ROWLOCK)"
    return TYPE_A_QUERY_TEMPLATE.format(table_hint=table_hint)


def build_type_b_query(isolation_level):
    """Build the Type B SELECT query for the selected isolation level."""
    if uses_readpast(isolation_level):
        table_hint = "WITH (READPAST)"
    else:
        table_hint = ""
    return TYPE_B_QUERY_TEMPLATE.format(table_hint=table_hint)


def parse_experiment(entry):
    """Parse a single experiment entry written as 'type_a,type_b'."""
    parts = [part.strip() for part in entry.split(",")]
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"Invalid experiment '{entry}'. Expected format: TypeAUsers,TypeBUsers"
        )

    try:
        type_a_users = int(parts[0])
        type_b_users = int(parts[1])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid experiment '{entry}'. User counts must be integers."
        ) from exc

    if type_a_users < 0 or type_b_users < 0:
        raise argparse.ArgumentTypeError(
            f"Invalid experiment '{entry}'. User counts must be non-negative."
        )

    return type_a_users, type_b_users


def parse_arguments():
    """Build the command-line interface."""
    parser = argparse.ArgumentParser(
        description="Simulate concurrent AdventureWorks transactions on SQL Server."
    )
    parser.add_argument(
        "--type-a-users",
        type=int,
        default=5,
        help="Number of Type A (UPDATE) user threads for a single run.",
    )
    parser.add_argument(
        "--type-b-users",
        type=int,
        default=5,
        help="Number of Type B (SELECT) user threads for a single run.",
    )
    parser.add_argument(
        "--experiments",
        nargs="+",
        type=parse_experiment,
        help=(
            "One or more experiment sizes in the form 'TypeAUsers,TypeBUsers'. "
            "Example: --experiments 1,1 5,5 10,10"
        ),
    )
    parser.add_argument(
        "--isolation-levels",
        nargs="+",
        default=["READ COMMITTED"],
        help=(
            "Isolation levels to test. Examples: 'READ COMMITTED' SERIALIZABLE ALL. "
            "Supported values: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE."
        ),
    )
    parser.add_argument(
        "--transactions-per-thread",
        type=int,
        default=TRANSACTIONS_PER_THREAD,
        help="How many transactions each thread executes.",
    )
    parser.add_argument(
        "--connection-timeout",
        type=int,
        default=DEFAULT_CONNECTION_TIMEOUT_SECONDS,
        help="Connection timeout in seconds for pyodbc.connect().",
    )
    parser.add_argument(
        "--query-timeout",
        type=int,
        default=DEFAULT_QUERY_TIMEOUT_SECONDS,
        help="Per-statement timeout in seconds for pyodbc cursors.",
    )
    parser.add_argument(
        "--lock-timeout-ms",
        type=int,
        default=DEFAULT_LOCK_TIMEOUT_MILLISECONDS,
        help="SQL Server lock timeout in milliseconds.",
    )
    parser.add_argument(
        "--thread-join-timeout",
        type=float,
        default=DEFAULT_JOIN_TIMEOUT_SECONDS,
        help="Maximum seconds to wait for each worker thread to finish.",
    )
    parser.add_argument(
        "--driver",
        default=DEFAULT_DRIVER,
        help="ODBC driver name. Example: 'ODBC Driver 18 for SQL Server'.",
    )
    parser.add_argument("--server", default=DEFAULT_SERVER, help="SQL Server host and port.")
    parser.add_argument("--database", default=DEFAULT_DATABASE, help="Database name.")
    parser.add_argument("--username", default=DEFAULT_USERNAME, help="Database username.")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Database password.")
    parser.add_argument(
        "--disable-pooling",
        action="store_true",
        help="Disable pyodbc connection pooling.",
    )
    parser.add_argument(
        "--csv-output",
        help="Optional path to write results as CSV.",
    )

    args = parser.parse_args()

    if args.type_a_users < 0 or args.type_b_users < 0:
        parser.error("--type-a-users and --type-b-users must be non-negative.")
    if args.transactions_per_thread <= 0:
        parser.error("--transactions-per-thread must be greater than zero.")
    if args.connection_timeout <= 0:
        parser.error("--connection-timeout must be greater than zero.")
    if args.query_timeout <= 0:
        parser.error("--query-timeout must be greater than zero.")
    if args.lock_timeout_ms <= 0:
        parser.error("--lock-timeout-ms must be greater than zero.")
    if args.thread_join_timeout <= 0:
        parser.error("--thread-join-timeout must be greater than zero.")

    isolation_values = []
    for entry in args.isolation_levels:
        if entry.upper() == "ALL":
            isolation_values = list(ISOLATION_LEVELS)
            break
        isolation_values.append(normalize_isolation_level(entry))
    args.isolation_levels = isolation_values

    return args


def is_deadlock_error(exception):
    """Return True when the database error indicates a deadlock victim."""
    message = str(exception).lower()
    return "deadlock victim" in message or "was deadlocked" in message or "40001" in message


def is_lock_timeout_error(exception):
    """Return True when SQL Server or ODBC reports a lock/query timeout."""
    message = str(exception).lower()
    return (
        "lock request time out period exceeded" in message
        or "error 1222" in message
        or "1222" in message
        or "timeout expired" in message
        or "query timeout expired" in message
        or "hyt00" in message
    )


def rollback_quietly(connection):
    """Best-effort rollback used during error handling."""
    if connection is None:
        return
    try:
        connection.rollback()
    except pyodbc.Error:
        pass


def close_quietly(resource):
    """Best-effort close used for cursors and connections."""
    if resource is None:
        return
    try:
        resource.close()
    except pyodbc.Error:
        pass


def execute_transaction(connection_string, isolation_level, query, fetch_results, args):
    """Run one database transaction with bounded execution time.

    The combination of pyodbc connection timeout and SQL Server LOCK_TIMEOUT
    ensures one transaction cannot block forever.
    """
    connection = None
    cursor = None
    try:
        connection = pyodbc.connect(
            connection_string,
            autocommit=False,
            timeout=args.connection_timeout,
        )
        cursor = connection.cursor()

        cursor.execute(f"SET LOCK_TIMEOUT {args.lock_timeout_ms}")
        cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(query)
        if fetch_results:
            cursor.fetchone()

        connection.commit()
        return {"deadlock": False, "lock_timeout": False, "error": None}
    except pyodbc.Error as exc:
        rollback_quietly(connection)

        if is_deadlock_error(exc):
            return {"deadlock": True, "lock_timeout": False, "error": None}
        if is_lock_timeout_error(exc):
            return {"deadlock": False, "lock_timeout": True, "error": None}
        return {"deadlock": False, "lock_timeout": False, "error": str(exc)}
    finally:
        close_quietly(cursor)
        close_quietly(connection)


def run_user(
    thread_name,
    connection_string,
    isolation_level,
    query,
    fetch_results,
    transactions,
    args,
):
    """Execute the workload for one simulated user thread safely."""
    start_time = time.perf_counter()
    deadlocks = 0
    lock_timeouts = 0
    unexpected_errors = []

    for iteration in range(transactions):
        transaction_started = time.perf_counter()
        try:
            outcome = execute_transaction(
                connection_string=connection_string,
                isolation_level=isolation_level,
                query=query,
                fetch_results=fetch_results,
                args=args,
            )
            if outcome["deadlock"]:
                deadlocks += 1
            elif outcome["lock_timeout"]:
                lock_timeouts += 1
            elif outcome["error"]:
                unexpected_errors.append(
                    f"{thread_name} transaction {iteration + 1}: {outcome['error']}"
                )
        except Exception as exc:  # pylint: disable=broad-except
            unexpected_errors.append(
                f"{thread_name} transaction {iteration + 1}: unexpected error: {exc}"
            )

        # This check documents when a transaction exceeded the intended bound.
        # In practice, cursor/lock timeouts should prevent this path.
        transaction_elapsed = time.perf_counter() - transaction_started
        if transaction_elapsed > args.query_timeout + 1:
            unexpected_errors.append(
                f"{thread_name} transaction {iteration + 1}: exceeded expected timeout "
                f"window ({transaction_elapsed:.2f}s)"
            )

    elapsed_seconds = time.perf_counter() - start_time
    return {
        "thread_name": thread_name,
        "elapsed_seconds": elapsed_seconds,
        "deadlocks": deadlocks,
        "lock_timeouts": lock_timeouts,
        "unexpected_errors": unexpected_errors,
        "completed": True,
    }


def run_type_a_user(thread_name, connection_string, isolation_level, transactions, args):
    """Worker for Type A users that issue UPDATE statements."""
    return run_user(
        thread_name=thread_name,
        connection_string=connection_string,
        isolation_level=isolation_level,
        query=build_type_a_query(isolation_level),
        fetch_results=False,
        transactions=transactions,
        args=args,
    )


def run_type_b_user(thread_name, connection_string, isolation_level, transactions, args):
    """Worker for Type B users that issue SELECT statements."""
    return run_user(
        thread_name=thread_name,
        connection_string=connection_string,
        isolation_level=isolation_level,
        query=build_type_b_query(isolation_level),
        fetch_results=True,
        transactions=transactions,
        args=args,
    )


def build_timeout_result(thread_name, join_timeout_seconds):
    """Create a synthetic result for a worker that did not finish in time."""
    return {
        "thread_name": thread_name,
        "elapsed_seconds": join_timeout_seconds,
        "deadlocks": 0,
        "lock_timeouts": 0,
        "unexpected_errors": [
            f"{thread_name}: worker thread exceeded join timeout of "
            f"{join_timeout_seconds:.2f}s"
        ],
        "completed": False,
    }


def run_simulation(type_a_users, type_b_users, isolation_level, connection_string, transactions, args):
    """Run one simulation for the given user counts and isolation level.

    Threads are joined with a timeout so the main program cannot block forever.
    Threads are also marked as daemon threads so the interpreter can exit even
    if a driver call becomes unresponsive.
    """
    type_a_results = []
    type_b_results = []
    result_lock = threading.Lock()
    threads = []
    thread_specs = []

    def worker(kind, index):
        try:
            if kind == "A":
                result = run_type_a_user(
                    thread_name=f"TypeA-{index}",
                    connection_string=connection_string,
                    isolation_level=isolation_level,
                    transactions=transactions,
                    args=args,
                )
                with result_lock:
                    type_a_results.append(result)
            else:
                result = run_type_b_user(
                    thread_name=f"TypeB-{index}",
                    connection_string=connection_string,
                    isolation_level=isolation_level,
                    transactions=transactions,
                    args=args,
                )
                with result_lock:
                    type_b_results.append(result)
        except Exception as exc:  # pylint: disable=broad-except
            fallback_result = {
                "thread_name": f"Type{kind}-{index}",
                "elapsed_seconds": 0.0,
                "deadlocks": 0,
                "lock_timeouts": 0,
                "unexpected_errors": [f"Type{kind}-{index}: worker crashed: {exc}"],
                "completed": False,
            }
            with result_lock:
                if kind == "A":
                    type_a_results.append(fallback_result)
                else:
                    type_b_results.append(fallback_result)

    for index in range(1, type_a_users + 1):
        thread = threading.Thread(
            target=worker,
            args=("A", index),
            name=f"TypeA-{index}",
            daemon=True,
        )
        threads.append(thread)
        thread_specs.append(("A", index, thread))

    for index in range(1, type_b_users + 1):
        thread = threading.Thread(
            target=worker,
            args=("B", index),
            name=f"TypeB-{index}",
            daemon=True,
        )
        threads.append(thread)
        thread_specs.append(("B", index, thread))

    for thread in threads:
        thread.start()

    join_deadline = time.perf_counter() + args.thread_join_timeout
    for kind, index, thread in thread_specs:
        remaining = max(0.0, join_deadline - time.perf_counter())
        thread.join(timeout=remaining)
        if thread.is_alive():
            timeout_result = build_timeout_result(thread.name, args.thread_join_timeout)
            with result_lock:
                if kind == "A":
                    type_a_results.append(timeout_result)
                else:
                    type_b_results.append(timeout_result)

    avg_duration_a = (
        statistics.mean(result["elapsed_seconds"] for result in type_a_results)
        if type_a_results
        else 0.0
    )
    avg_duration_b = (
        statistics.mean(result["elapsed_seconds"] for result in type_b_results)
        if type_b_results
        else 0.0
    )

    return {
        "isolation_level": isolation_level,
        "type_a_users": type_a_users,
        "type_b_users": type_b_users,
        "avg_duration_a": avg_duration_a,
        "deadlocks_a": sum(result["deadlocks"] for result in type_a_results),
        "lock_timeouts_a": sum(result["lock_timeouts"] for result in type_a_results),
        "avg_duration_b": avg_duration_b,
        "deadlocks_b": sum(result["deadlocks"] for result in type_b_results),
        "lock_timeouts_b": sum(result["lock_timeouts"] for result in type_b_results),
        "errors_a": [error for result in type_a_results for error in result["unexpected_errors"]],
        "errors_b": [error for result in type_b_results for error in result["unexpected_errors"]],
    }


def print_results_table(isolation_level, results):
    """Print one result table for a specific isolation level."""
    print(f"\nIsolation Level: {isolation_level}")
    print(
        "TypeAUsers | TypeBUsers | AvgDurationA(s) | DeadlocksA | "
        "AvgDurationB(s) | DeadlocksB"
    )
    print("-" * 83)
    for result in results:
        print(
            f"{result['type_a_users']:10d} | "
            f"{result['type_b_users']:10d} | "
            f"{result['avg_duration_a']:15.4f} | "
            f"{result['deadlocks_a']:10d} | "
            f"{result['avg_duration_b']:15.4f} | "
            f"{result['deadlocks_b']:10d}"
        )


def write_csv(output_path, results):
    """Write experiment results to a CSV file without extra dependencies."""
    with open(output_path, "w", encoding="utf-8") as csv_file:
        csv_file.write(
            "IsolationLevel,TypeAUsers,TypeBUsers,AvgDurationA,DeadlocksA,"
            "AvgDurationB,DeadlocksB,LockTimeoutsA,LockTimeoutsB\n"
        )
        for result in results:
            csv_file.write(
                f"{result['isolation_level']},"
                f"{result['type_a_users']},"
                f"{result['type_b_users']},"
                f"{result['avg_duration_a']:.6f},"
                f"{result['deadlocks_a']},"
                f"{result['avg_duration_b']:.6f},"
                f"{result['deadlocks_b']},"
                f"{result['lock_timeouts_a']},"
                f"{result['lock_timeouts_b']}\n"
            )


def main():
    """Program entry point."""
    print("Simulation started")
    args = parse_arguments()
    pyodbc.pooling = not args.disable_pooling

    experiments = args.experiments or [(args.type_a_users, args.type_b_users)]
    connection_string = build_connection_string(args)
    all_results = []

    for isolation_level in args.isolation_levels:
        level_results = []
        for type_a_users, type_b_users in experiments:
            print("Running experiment:", type_a_users, type_b_users)
            result = run_simulation(
                type_a_users=type_a_users,
                type_b_users=type_b_users,
                isolation_level=isolation_level,
                connection_string=connection_string,
                transactions=args.transactions_per_thread,
                args=args,
            )
            all_results.append(result)
            level_results.append(result)
        print_results_table(isolation_level, level_results)

        errors = [
            error
            for result in level_results
            for error in (result["errors_a"] + result["errors_b"])
        ]
        if errors:
            print("\nHandled errors encountered during this isolation level:")
            for error in errors:
                print(f"- {error}")

    if args.csv_output:
        write_csv(args.csv_output, all_results)
        print(f"\nCSV results written to: {args.csv_output}")


if __name__ == "__main__":
    main()
