# Database Concurrency Simulation

This project investigates how different transaction isolation levels affect database performance, concurrency behavior, and deadlock frequency.

The simulation executes concurrent transactions against a Microsoft SQL Server AdventureWorks database using Python threading. It measures execution time and analyzes system behavior under different concurrency workloads.

This project was developed as part of the Advanced Topics in Database Systems course.

---

## Project Objectives

The goal of this project is to analyze how different transaction isolation levels influence:

- Transaction execution time
- Database concurrency behavior
- Deadlock frequency
- Lock contention
- Overall system performance

The following isolation levels were tested:

- READ UNCOMMITTED
- READ COMMITTED
- REPEATABLE READ
- SERIALIZABLE

Experiments were also conducted with and without database indexes to evaluate their impact on query performance.

---

## Technologies Used

- Python 3
- Microsoft SQL Server
- AdventureWorks sample database
- `pyodbc`
- Python threading
- Matplotlib for performance graphs

---

## Simulation Architecture

The system simulates a multi-user database environment using Python threads. Each thread represents a concurrent database user executing transactions.

Two types of transactions were implemented:

### Type A Transaction (Write-Heavy)

Updates records in the `Sales.SalesOrderDetail` table to simulate write workloads.

Example query:

```sql
UPDATE Sales.SalesOrderDetail
SET OrderQty = OrderQty
WHERE SalesOrderID = ?
```

### Type B Transaction (Read-Heavy)

Retrieves aggregated order data to simulate read workloads.

Example query:

```sql
SELECT SalesOrderID, SUM(OrderQty)
FROM Sales.SalesOrderDetail
GROUP BY SalesOrderID
```

### Concurrency Simulation

Concurrent transactions are executed using Python threads.

The simulation runs experiments with different numbers of concurrent users:

- 1 user
- 3 users
- 5 users

Each experiment is repeated under all isolation levels to measure performance differences.

---

## Isolation Levels Tested

| Isolation Level | Behavior |
| --- | --- |
| READ UNCOMMITTED | Allows dirty reads but offers highest performance |
| READ COMMITTED | Prevents dirty reads but allows non-repeatable reads |
| REPEATABLE READ | Prevents non-repeatable reads but allows phantom reads |
| SERIALIZABLE | Strongest isolation but highest locking overhead |

---

## Index Experiments

To evaluate the effect of indexing on performance, the simulation was executed under two database configurations:

### Without Indexes

Queries rely on full table scans.

### With Indexes

Indexes were created on frequently accessed columns:

- `SalesOrderDetail(SalesOrderID)`
- `SalesOrderHeader(OrderDate)`
- `SalesOrderHeader(OnlineOrderFlag)`

This allows SQL Server to use index lookups instead of full scans, improving performance.

---

## Running the Simulation

Run the simulation using the following command:

```bash
python3 database_simulation.py \
  --experiments 1,1 3,3 5,5 \
  --isolation-levels ALL \
  --csv-output results.csv
```

This command executes all experiments and stores results in a CSV file.

---

## Example Output

The simulation produces results such as:

```text
Isolation Level | TypeAUsers | TypeBUsers | AvgDurationA | DeadlocksA | AvgDurationB | DeadlocksB
```

The results are exported to a CSV dataset for further analysis.

---

## Performance Graphs

The project generates graphs showing:

- Transaction duration vs number of users
- Isolation level impact on performance
- Deadlock frequency

Example graphs include:

- Type A Transaction Duration
- Type B Transaction Duration
- Deadlock Analysis

---

## Repository Structure

```text
database-concurrency-simulation
├── database_simulation.py
├── plot_results.py
├── results.csv
├── results_with_index.csv
├── results_no_index.csv
├── README.md
└── ScreenShots
    ├── graph_typeA_duration.png
    ├── graph_typeB_duration.png
    └── graph_deadlocks.png
```

---

## Key Findings

The experiments demonstrate several important observations:

- Transaction duration increases as the number of concurrent users grows.
- Stronger isolation levels introduce additional locking overhead.
- SERIALIZABLE provides the highest consistency but the lowest performance.
- Indexes significantly improve query performance by reducing full table scans.
- Deadlocks are more likely to occur under higher isolation levels.

---

```text
https://github.com/hasan4adnan/database-concurrency-simulation
```

---

## License

This project is provided for educational and research purposes.
