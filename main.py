import time
from statistics import mean
import psycopg2
from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np
from rich.console import Console
from rich.table import Table

# -------------------------------
# CONFIGURATION
# -------------------------------
# PostgreSQL
PG_CONFIG = {
    "dbname": "testdb",
    "user": "postgres",
    "password": "kalmin123",
    "host": "localhost",
    "port": 5432
}

# MongoDB
MONGO_CONFIG = {
    "user": "admin",
    "password": "kalmin123",
    "host": "localhost",
    "port": 27017,
    "db": "perf_test_db",
    "collection": "perf_test"
}

# Benchmark settings
SINGLE_RUNS = 10
BULK_SIZE = 10000

console = Console()

# -------------------------------
# UTILITY FUNCTIONS
# -------------------------------
def measure(func, repeat=SINGLE_RUNS):
    """Measure average, min, max execution time in ms"""
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        func()
        times.append((time.perf_counter() - start) * 1000)
    return {"avg_ms": round(mean(times), 3),
            "min_ms": round(min(times), 3),
            "max_ms": round(max(times), 3)}

def throughput(ms, count):
    """Compute throughput (rows/docs per second)"""
    seconds = ms / 1000
    return round(count / seconds, 2) if seconds > 0 else 0

# -------------------------------
# POSTGRESQL BENCHMARK
# -------------------------------
def benchmark_postgres():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS perf_test (id SERIAL PRIMARY KEY, name TEXT, age INT);")
    conn.commit()
    cur.execute("DELETE FROM perf_test;")
    conn.commit()

    pg_metrics = {}

    # Single CRUD
    pg_metrics['Single Create'] = measure(lambda: cur.execute(
        "INSERT INTO perf_test (name, age) VALUES (%s, %s)", ("Kalmin", 25)) or conn.commit())
    pg_metrics['Single Read'] = measure(lambda: cur.execute(
        "SELECT * FROM perf_test WHERE name=%s", ("Kalmin",)) or cur.fetchall())
    pg_metrics['Single Update'] = measure(lambda: cur.execute(
        "UPDATE perf_test SET age=%s WHERE name=%s", (30, "Kalmin")) or conn.commit())
    pg_metrics['Single Delete'] = measure(lambda: cur.execute(
        "DELETE FROM perf_test WHERE name=%s", ("Kalmin",)) or conn.commit())

    # Bulk CRUD
    def bulk_create():
        cur.executemany("INSERT INTO perf_test (name, age) VALUES (%s,%s)",
                        [(f"User{i}", i % 100) for i in range(BULK_SIZE)])
        conn.commit()
    pg_metrics['Bulk Create'] = measure(bulk_create, repeat=1)
    pg_metrics['Bulk Create']['throughput'] = throughput(pg_metrics['Bulk Create']['avg_ms'], BULK_SIZE)

    def bulk_read():
        cur.execute("SELECT * FROM perf_test")
        cur.fetchall()
    pg_metrics['Bulk Read'] = measure(bulk_read, repeat=1)
    pg_metrics['Bulk Read']['throughput'] = throughput(pg_metrics['Bulk Read']['avg_ms'], BULK_SIZE)

    def bulk_update():
        cur.execute("UPDATE perf_test SET age = age + 1")
        conn.commit()
    pg_metrics['Bulk Update'] = measure(bulk_update, repeat=1)
    pg_metrics['Bulk Update']['throughput'] = throughput(pg_metrics['Bulk Update']['avg_ms'], BULK_SIZE)

    def bulk_delete():
        cur.execute("DELETE FROM perf_test")
        conn.commit()
    pg_metrics['Bulk Delete'] = measure(bulk_delete, repeat=1)
    pg_metrics['Bulk Delete']['throughput'] = throughput(pg_metrics['Bulk Delete']['avg_ms'], BULK_SIZE)

    cur.close()
    conn.close()
    return pg_metrics

# -------------------------------
# MONGODB BENCHMARK
# -------------------------------
def benchmark_mongo():
    uri = f"mongodb://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
    client = MongoClient(uri)
    db = client[MONGO_CONFIG['db']]
    collection = db[MONGO_CONFIG['collection']]
    collection.drop()

    mongo_metrics = {}

    # Single CRUD
    mongo_metrics['Single Create'] = measure(lambda: collection.insert_one({"name": "Kalmin", "age": 25}))
    mongo_metrics['Single Read'] = measure(lambda: list(collection.find({"name": "Kalmin"})))
    mongo_metrics['Single Update'] = measure(lambda: collection.update_one({"name": "Kalmin"}, {"$set": {"age": 30}}))
    mongo_metrics['Single Delete'] = measure(lambda: collection.delete_one({"name": "Kalmin"}))

    # Bulk CRUD
    def bulk_create():
        collection.insert_many([{"name": f"User{i}", "age": i % 100} for i in range(BULK_SIZE)])
    mongo_metrics['Bulk Create'] = measure(bulk_create, repeat=1)
    mongo_metrics['Bulk Create']['throughput'] = throughput(mongo_metrics['Bulk Create']['avg_ms'], BULK_SIZE)

    def bulk_read():
        list(collection.find({}))
    mongo_metrics['Bulk Read'] = measure(bulk_read, repeat=1)
    mongo_metrics['Bulk Read']['throughput'] = throughput(mongo_metrics['Bulk Read']['avg_ms'], BULK_SIZE)

    def bulk_update():
        collection.update_many({}, {"$inc": {"age": 1}})
    mongo_metrics['Bulk Update'] = measure(bulk_update, repeat=1)
    mongo_metrics['Bulk Update']['throughput'] = throughput(mongo_metrics['Bulk Update']['avg_ms'], BULK_SIZE)

    def bulk_delete():
        collection.delete_many({})
    mongo_metrics['Bulk Delete'] = measure(bulk_delete, repeat=1)
    mongo_metrics['Bulk Delete']['throughput'] = throughput(mongo_metrics['Bulk Delete']['avg_ms'], BULK_SIZE)

    client.close()
    return mongo_metrics

# -------------------------------
# RUN BENCHMARKS
# -------------------------------
console.print("[bold blue]Running PostgreSQL Benchmark...[/bold blue]")
pg_results = benchmark_postgres()
console.print("[bold green]Running MongoDB Benchmark...[/bold green]")
mongo_results = benchmark_mongo()

# -------------------------------
# RICH TABLE DISPLAY
# -------------------------------
table = Table(title=f"CRUD Performance Comparison (Single & Bulk, {BULK_SIZE} bulk)")
table.add_column("Operation", justify="left", style="cyan")
table.add_column("PostgreSQL Avg (ms)", justify="center", style="green")
table.add_column("PostgreSQL Throughput", justify="center", style="green")
table.add_column("MongoDB Avg (ms)", justify="center", style="magenta")
table.add_column("MongoDB Throughput", justify="center", style="magenta")

for op in pg_results.keys():
    pg_avg = pg_results[op]['avg_ms']
    pg_tp = str(pg_results[op].get('throughput', '-'))
    mongo_avg = mongo_results[op]['avg_ms']
    mongo_tp = str(mongo_results[op].get('throughput', '-'))
    table.add_row(op, str(pg_avg), pg_tp, str(mongo_avg), mongo_tp)

console.print(table)

# -------------------------------
# CONCLUSIONS
# -------------------------------
def generate_conclusion(pg, mongo):
    conclusions = []

    # Single CRUD
    single_ops = ['Single Create', 'Single Read', 'Single Update', 'Single Delete']
    for op in single_ops:
        faster = "MongoDB" if mongo[op]['avg_ms'] < pg[op]['avg_ms'] else "PostgreSQL"
        conclusions.append(f"{op}: {faster} is faster ({pg[op]['avg_ms']}ms vs {mongo[op]['avg_ms']}ms)")

    # Bulk CRUD
    bulk_ops = ['Bulk Create', 'Bulk Read', 'Bulk Update', 'Bulk Delete']
    for op in bulk_ops:
        faster = "MongoDB" if mongo[op]['avg_ms'] < pg[op]['avg_ms'] else "PostgreSQL"
        conclusions.append(f"{op}: {faster} is faster "
                           f"({pg[op]['avg_ms']}ms vs {mongo[op]['avg_ms']}ms), "
                           f"throughput: {pg[op].get('throughput','-')} vs {mongo[op].get('throughput','-')} docs/sec")

    return conclusions

console.print("\n[bold underline]Conclusions:[/bold underline]")
for line in generate_conclusion(pg_results, mongo_results):
    console.print(line)

# -------------------------------
# MATPLOTLIB VISUALIZATION
# -------------------------------
operations = list(pg_results.keys())
pg_avg = [pg_results[op]['avg_ms'] for op in operations]
mongo_avg = [mongo_results[op]['avg_ms'] for op in operations]

bulk_ops = ['Bulk Create', 'Bulk Read', 'Bulk Update', 'Bulk Delete']
pg_tp = [pg_results[op]['throughput'] for op in bulk_ops]
mongo_tp = [mongo_results[op]['throughput'] for op in bulk_ops]

x = np.arange(len(operations))
width = 0.35

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

# Top: Avg Time
bars1 = ax1.bar(x - width/2, pg_avg, width, label='PostgreSQL', color='skyblue')
bars2 = ax1.bar(x + width/2, mongo_avg, width, label='MongoDB', color='salmon')
ax1.set_ylabel('Average Time (ms)')
ax1.set_title('CRUD Operations Average Time')
ax1.set_xticks(x)
ax1.set_xticklabels(operations, rotation=30, ha='right')
ax1.legend()
for bar in bars1 + bars2:
    ax1.annotate(f'{bar.get_height():.1f}', xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                 xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=8)

# Bottom: Bulk Throughput
x_bulk = np.arange(len(bulk_ops))
bars1 = ax2.bar(x_bulk - width/2, pg_tp, width, label='PostgreSQL', color='skyblue')
bars2 = ax2.bar(x_bulk + width/2, mongo_tp, width, label='MongoDB', color='salmon')
ax2.set_ylabel('Throughput (rows/documents/sec)')
ax2.set_title(f'Bulk Operations Throughput (Bulk Size = {BULK_SIZE})')
ax2.set_xticks(x_bulk)
ax2.set_xticklabels(bulk_ops, rotation=30, ha='right')
ax2.legend()
for bar in bars1 + bars2:
    ax2.annotate(f'{int(bar.get_height())}', xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                 xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=8)

plt.tight_layout()
plt.show()

