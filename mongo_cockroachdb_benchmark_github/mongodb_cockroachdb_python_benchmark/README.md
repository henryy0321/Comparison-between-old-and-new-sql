# MongoDB vs CockroachDB Benchmark

A small, reproducible coursework project to compare **MongoDB (NoSQL)** and **CockroachDB (NewSQL)** using the same test records and workload mixes.

The project uses:
- MongoDB 8.0 with the official PyMongo Python driver.
- CockroachDB 24.3.0 with Psycopg 3 (CockroachDB supports the PostgreSQL wire protocol).
- Docker Compose to run both databases on one computer.
- Python threads to simulate concurrent clients.
- Docker statistics to sample database-container CPU and memory use.

> **Important methodology note:** This project implements YCSB-style workload mixes in Python. It does **not** run the official YCSB software. The released YCSB 0.17.0 MongoDB binding depends on an old MongoDB Java driver, so this project uses current Python database drivers instead. Update the wording in your report to describe this as a Python benchmark runner using YCSB-style workloads, unless your lecturer specifically requires the official YCSB framework.

## 1. Project files

```text
mongodb-cockroachdb-benchmark/
├── config/
│   ├── cockroach_schema.sql
│   └── workloads.json
├── results/
│   ├── charts/
│   ├── raw/
│   └── README.md
├── scripts/
│   ├── analyze_results.py
│   └── run_benchmark.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## 2. Install the required software

Install:
- Docker Desktop (with Docker Compose).
- Python 3.10 or later.

Open a terminal in this project folder.

## 3. Start the databases

```bash
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

CockroachDB runs as a **single-node local test instance**. Its insecure mode is only for local coursework testing, not for production or a public server.

## 4. Install the Python packages

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

The default connection settings are:

```text
MongoDB:     mongodb://localhost:27017
CockroachDB: postgresql://root@localhost:26257/ycsb?sslmode=disable
```

You can override them with environment variables `MONGODB_URI` and `COCKROACH_DSN`.

## 5. Run a small test first

This checks that both databases connect, data can be loaded, and benchmark output can be produced.

```bash
python scripts/run_benchmark.py --quick
```

The quick test uses 10,000 records, 1 and 5 clients, and 100 measured operations per client. **Do not use quick-test results as the final results for the assignment.**

## 6. Run the full experiment

```bash
python scripts/run_benchmark.py
```

The default full experiment uses:

| Setting | Values |
|---|---|
| Databases | MongoDB, CockroachDB |
| Dataset sizes | 100,000; 1,000,000; 10,000,000 records |
| Workloads | Read-heavy; mixed; write-heavy |
| Concurrent clients | 1; 10; 50; 100; 500 |
| Operations per client | 500 measured operations |
| Warm-up | 20 operations per client, excluded from latency/throughput measurements |
| Record fields | 10 fields, each 100 characters |

The workload mixes are:

| Workload | Reads | Updates |
|---|---:|---:|
| Read-heavy | 95% | 5% |
| Mixed | 50% | 50% |
| Write-heavy | 5% | 95% |

Writes are **updates** to existing records. This keeps the record count unchanged during the benchmark. Both databases read all ten fields and update all ten fields.

To run only one database, use:

```bash
python scripts/run_benchmark.py --db mongodb
python scripts/run_benchmark.py --db cockroachdb
```

To choose smaller test sizes manually:

```bash
python scripts/run_benchmark.py --datasets 100000 --clients 1,10,50 --operations-per-client 500
```

The 10-million-record dataset contains ten 100-character fields per record. It can require substantial disk space and time. Make sure your computer has enough available storage before running the full experiment.

## 7. Generate summary tables and charts

After the benchmark finishes, run:

```bash
python scripts/analyze_results.py
```

The script creates:
- `results/benchmark_results.csv` — one row for each benchmark run.
- `results/summary.csv` — grouped results for comparison.
- `results/charts/` — charts for throughput, p95 latency, CPU usage, and memory usage.
- `results/raw/` — raw JSON details for each run.

## 8. What each result means

| Metric | Meaning | Better direction |
|---|---|---|
| p95 latency (ms) | Time taken by 95% of measured operations | Lower |
| Average latency (ms) | Average time for one operation | Lower |
| Throughput (ops/sec) | Successful operations completed per second | Higher |
| Average CPU (%) | Average CPU usage reported for the database container during the run | Lower, when throughput is comparable |
| Average memory (MiB) | Average memory used by the database container during the run | Lower, when throughput is comparable |

CPU and memory are sampled using `docker stats`. Very short runs may not provide enough samples, so use the full experiment for more useful resource measurements.

## 9. Important limitations to report

1. The experiment compares only MongoDB and CockroachDB. Your original assessment document also lists PostgreSQL as a third database. Confirm this scope change with your group/lecturer before submitting.
2. CockroachDB runs as a single node here. This test does not measure replication or consistency propagation between multiple nodes.
3. Workloads are generated by this Python script using YCSB-style read/update proportions; the official YCSB framework is not run.
4. The tests use a synthetic dataset and a single computer. Results may differ in real-world, multi-node deployments.
5. Database drivers and database-specific implementations can affect results. Record the exact software versions and computer hardware used for your assessment.
6. Run each configuration more than once for a stronger study. The script appends each new run to the results CSV; the analysis script averages repeated runs with the same configuration.

## 10. What to upload to GitHub

Upload the project source files and, after you have run the experiment, the generated `benchmark_results.csv`, `summary.csv`, and charts. Do not upload invented or predicted measurements. Keep the raw JSON files if your lecturer wants supporting evidence; otherwise they can be large.

## References for the tools

- MongoDB PyMongo documentation: https://www.mongodb.com/docs/languages/python/pymongo-driver/current/
- CockroachDB Psycopg 3 guide: https://www.cockroachlabs.com/docs/stable/build-a-python-app-with-cockroachdb-psycopg3
- CockroachDB PostgreSQL wire protocol: https://github.com/cockroachdb/cockroach
- YCSB workload concepts and operation proportions: https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties
