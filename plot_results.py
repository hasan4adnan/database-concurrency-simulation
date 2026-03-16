import pandas as pd
import matplotlib.pyplot as plt

# CSV dosyasını oku
df = pd.read_csv("results.csv")

# Users sayısını tek kolonda toplamak için
df["Users"] = df["TypeAUsers"]

# -----------------------------
# Grafik 1: Users vs AvgDuration (Type A)
# -----------------------------
plt.figure()

for level in df["IsolationLevel"].unique():
    subset = df[df["IsolationLevel"] == level]
    plt.plot(subset["Users"], subset["AvgDurationA"], marker='o', label=level)

plt.xlabel("Number of Users")
plt.ylabel("Avg Duration Type A (seconds)")
plt.title("Users vs Avg Duration (Type A)")
plt.legend()
plt.grid(True)

plt.savefig("graph_typeA_duration.png")


# -----------------------------
# Grafik 2: Users vs AvgDuration (Type B)
# -----------------------------
plt.figure()

for level in df["IsolationLevel"].unique():
    subset = df[df["IsolationLevel"] == level]
    plt.plot(subset["Users"], subset["AvgDurationB"], marker='o', label=level)

plt.xlabel("Number of Users")
plt.ylabel("Avg Duration Type B (seconds)")
plt.title("Users vs Avg Duration (Type B)")
plt.legend()
plt.grid(True)

plt.savefig("graph_typeB_duration.png")


# -----------------------------
# Grafik 3: Isolation Level vs Deadlocks
# -----------------------------
deadlocks = df.groupby("IsolationLevel")["DeadlocksB"].sum()

plt.figure()
deadlocks.plot(kind="bar")

plt.xlabel("Isolation Level")
plt.ylabel("Total Deadlocks")
plt.title("Deadlocks by Isolation Level")

plt.savefig("graph_deadlocks.png")

print("Graphs created successfully.")
