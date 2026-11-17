#!/usr/bin/env python3
"""Benchmark MongoDB against CockroachDB using repeatable YCSB-style workloads.

This is a small Python benchmark runner. It uses PyMongo for MongoDB and
Psycopg 3 for CockroachDB, with the same logical records and operation mixes.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
RAW = RESULTS / "raw"
CSV_FILE = RESULTS / "benchmark_results.csv"
WORKLOAD_FILE = ROOT / "config" / "workloads.json"

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
COCKROACH_DSN = os.getenv(
    "COCKROACH_DSN",
    "postgresql://root@localhost:26257/ycsb?sslmode=disable",
)
CONTAINERS = {
    "mongodb": "benchmark-mongodb",
    "cockroachdb": "benchmark-cockroachdb",
}
FIELDS = [f"field{i}" for i in range(10)]
FIELD_VALUE_LENGTH = 100
BATCH_SIZE = 1000
CSV_FIELDS = [
    "timestamp_utc",
    "database",
    "dataset_records",
    "workload",
    "clients",
    "operations_per_client",
    "successful_operations",
    "errors",
    "run_time_seconds",
    "p95_latency_ms",
    "average_latency_ms",
    "throughput_ops_sec",
    "average_cpu_percent",
    "average_memory_mib",
    "status",
    "raw_log",
]


def load_workloads() -> dict[str, dict[str, float]]:
    with WORKLOAD_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def run_cli(command: list[str], timeout: int = 30) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Command not found: {command[0]}") from exc


def docker_exec(container: str, command: list[str], timeout: int = 60) -> str:
    result = run_cli(["docker", "exec", container, *command], timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stdout.strip() or f"docker exec failed for {container}")
    return result.stdout


def wait_for_databases(timeout_seconds: int = 120) -> None:
    """Wait until both database containers accept local health checks."""
    deadline = time.time() + timeout_seconds
    mongo_ready = False
    crdb_ready = False
    last_errors: list[str] = []

    while time.time() < deadline and not (mongo_ready and crdb_ready):
        if not mongo_ready:
            try:
                docker_exec(
                    CONTAINERS["mongodb"],
                    ["mongosh", "--quiet", "--eval", "db.adminCommand('ping').ok"],
                    timeout=10,
                )
                mongo_ready = True
            except Exception as exc:
                last_errors.append(f"MongoDB: {exc}")

        if not crdb_ready:
            try:
                docker_exec(
                    CONTAINERS["cockroachdb"],
                    [
                        "./cockroach", "sql", "--insecure", "--host=localhost:26257",
                        "--execute=SELECT 1",
                    ],
                    timeout=10,
                )
                crdb_ready = True
            except Exception as exc:
                last_errors.append(f"CockroachDB: {exc}")

        if not (mongo_ready and crdb_ready):
            time.sleep(3)

    if not (mongo_ready and crdb_ready):
        detail = "\n".join(last_errors[-6:])
        raise RuntimeError(
            "The databases did not become ready. Run `docker compose logs` to inspect them.\n"
            + detail
        )


def import_database_drivers():
    """Import drivers after argument parsing so --help works without dependencies."""
    try:
        from pymongo import MongoClient
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "Python packages are missing. Run: python -m pip install -r requirements.txt"
        ) from exc
    return MongoClient, psycopg


def create_mongo_client(MongoClient):
    return MongoClient(
        MONGO_URI,
        maxPoolSize=600,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=10000,
        appname="MongoCockroachBenchmark",
    )


def create_cockroach_database(psycopg) -> None:
    """Create the benchmark database if needed, using the default root user."""
    admin_dsn = COCKROACH_DSN.replace("/ycsb?", "/defaultdb?")
    with psycopg.connect(admin_dsn, autocommit=True, connect_timeout=30) as conn:
        conn.execute("CREATE DATABASE IF NOT EXISTS ycsb")


def reset_and_load_mongodb(MongoClient, record_count: int) -> None:
    client = create_mongo_client(MongoClient)
    try:
        db = client["ycsb"]
        if "usertable" in db.list_collection_names():
            db.drop_collection("usertable")
        collection = db["usertable"]

        print(f"  Loading MongoDB with {record_count:,} records...")
        for start in range(0, record_count, BATCH_SIZE):
            stop = min(start + BATCH_SIZE, record_count)
            batch = []
            for number in range(start, stop):
                key = f"user{number:010d}"
                document = {"_id": key}
                for field in FIELDS:
                    document[field] = make_field_value(number, field)
                batch.append(document)
            collection.insert_many(batch, ordered=True)
            if stop % 100000 == 0 or stop == record_count:
                print(f"    MongoDB loaded {stop:,}/{record_count:,}")
    finally:
        client.close()


def reset_and_load_cockroach(psycopg, record_count: int) -> None:
    print(f"  Loading CockroachDB with {record_count:,} records...")
    with psycopg.connect(COCKROACH_DSN, autocommit=True, connect_timeout=30) as conn:
        conn.execute("DROP TABLE IF EXISTS usertable")
        field_definitions = ", ".join(f"{field} STRING NOT NULL" for field in FIELDS)
        conn.execute(
            f"CREATE TABLE usertable (id STRING PRIMARY KEY, {field_definitions})"
        )

    placeholders = ", ".join(["%s"] * (1 + len(FIELDS)))
    columns = ", ".join(["id", *FIELDS])
    insert_sql = f"INSERT INTO usertable ({columns}) VALUES ({placeholders})"

    with psycopg.connect(COCKROACH_DSN, autocommit=False, connect_timeout=30) as conn:
        for start in range(0, record_count, BATCH_SIZE):
            stop = min(start + BATCH_SIZE, record_count)
            rows = []
            for number in range(start, stop):
                key = f"user{number:010d}"
                rows.append((key, *(make_field_value(number, field) for field in FIELDS)))
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, rows)
            conn.commit()
            if stop % 100000 == 0 or stop == record_count:
                print(f"    CockroachDB loaded {stop:,}/{record_count:,}")


def make_field_value(record_number: int, field_name: str) -> str:
    """Return a deterministic 100-character field value for both databases."""
    seed_text = f"record={record_number};field={field_name};"
    return (seed_text + ("x" * FIELD_VALUE_LENGTH))[:FIELD_VALUE_LENGTH]


def percentile(values: list[float], percent: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil((percent / 100.0) * len(ordered)) - 1)
    return ordered[index]


def parse_memory_mib(text_value: str) -> Optional[float]:
    first = text_value.split("/")[0].strip()
    match = re.match(r"([0-9.]+)\s*(B|KiB|MiB|GiB|TiB)?", first, re.IGNORECASE)
    if not match:
        return None
    amount = float(match.group(1))
    unit = (match.group(2) or "B").lower()
    scales = {
        "b": 1 / (1024 * 1024),
        "kib": 1 / 1024,
        "mib": 1,
        "gib": 1024,
        "tib": 1024 * 1024,
    }
    return amount * scales[unit]


class DockerStatsSampler:
    """Sample container CPU and memory usage while a benchmark is running."""

    def __init__(self, container_name: str, interval_seconds: float = 0.5):
        self.container_name = container_name
        self.interval_seconds = interval_seconds
        self.cpu_values: list[float] = []
        self.memory_values: list[float] = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> tuple[Optional[float], Optional[float]]:
        self.stop_event.set()
        self.thread.join(timeout=8)
        cpu_average = mean(self.cpu_values) if self.cpu_values else None
        memory_average = mean(self.memory_values) if self.memory_values else None
        return cpu_average, memory_average

    def _run(self) -> None:
        while not self.stop_event.is_set():
            result = run_cli([
                "docker", "stats", "--no-stream", "--format",
                "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}", self.container_name,
            ], timeout=15)
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    parts = line.strip().split("|", 2)
                    if len(parts) != 3:
                        continue
                    try:
                        self.cpu_values.append(float(parts[1].replace("%", "").strip()))
                    except ValueError:
                        pass
                    memory = parse_memory_mib(parts[2])
                    if memory is not None:
                        self.memory_values.append(memory)
            self.stop_event.wait(self.interval_seconds)


def perform_mongo_operation(collection, operation: str, key: str, update_value: str) -> None:
    if operation == "read":
        projection = {field: 1 for field in FIELDS}
        projection["_id"] = 1
        collection.find_one({"_id": key}, projection)
    else:
        update = {field: update_value for field in FIELDS}
        collection.update_one({"_id": key}, {"$set": update})


def perform_cockroach_operation(conn, operation: str, key: str, update_value: str) -> None:
    if operation == "read":
        columns = ", ".join(FIELDS)
        conn.execute(f"SELECT {columns} FROM usertable WHERE id = %s", (key,)).fetchone()
    else:
        assignments = ", ".join(f"{field} = %s" for field in FIELDS)
        parameters = tuple([update_value] * len(FIELDS) + [key])
        conn.execute(f"UPDATE usertable SET {assignments} WHERE id = %s", parameters)


def run_worker(
    worker_id: int,
    database: str,
    record_count: int,
    operations_per_client: int,
    warmup_operations: int,
    read_ratio: float,
    start_barrier: threading.Barrier,
    shared_mongo_client=None,
) -> tuple[list[float], int, list[str]]:
    rng = random.Random(20261117 + worker_id)
    latencies_ms: list[float] = []
    errors: list[str] = []
    successful = 0
    conn = None

    # CockroachDB uses one connection per worker. PyMongo's shared client is thread-safe.
    psycopg = None
    try:
        if database == "mongodb":
            if shared_mongo_client is None:
                raise RuntimeError("A shared MongoDB client was not provided to the worker.")
            collection = shared_mongo_client["ycsb"]["usertable"]
        else:
            import psycopg as psycopg_module
            psycopg = psycopg_module
            conn = psycopg.connect(COCKROACH_DSN, autocommit=True, connect_timeout=30)

        def one_operation(measure: bool) -> None:
            nonlocal successful
            operation = "read" if rng.random() < read_ratio else "update"
            record_number = rng.randrange(record_count)
            key = f"user{record_number:010d}"
            update_value = f"worker={worker_id};time={time.time_ns()}".ljust(FIELD_VALUE_LENGTH, "z")[:FIELD_VALUE_LENGTH]
            started = time.perf_counter_ns()
            if database == "mongodb":
                perform_mongo_operation(collection, operation, key, update_value)
            else:
                perform_cockroach_operation(conn, operation, key, update_value)
            elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
            successful += 1
            if measure:
                latencies_ms.append(elapsed_ms)

        for _ in range(warmup_operations):
            one_operation(measure=False)

        start_barrier.wait(timeout=120)
        for _ in range(operations_per_client):
            try:
                one_operation(measure=True)
            except Exception as exc:  # keep count and preserve examples for the result file
                errors.append(f"worker {worker_id}: {type(exc).__name__}: {exc}")
    except Exception as exc:
        errors.append(f"worker {worker_id} setup: {type(exc).__name__}: {exc}")
        try:
            start_barrier.abort()
        except Exception:
            pass
    finally:
        if conn is not None:
            conn.close()

    return latencies_ms, successful, errors


def run_benchmark(
    database: str,
    record_count: int,
    workload_name: str,
    clients: int,
    operations_per_client: int,
    warmup_operations: int,
    read_ratio: float,
    collect_stats: bool,
) -> dict[str, Any]:
    start_time_holder: dict[str, float] = {}
    sampler = DockerStatsSampler(CONTAINERS[database]) if collect_stats else None
    shared_mongo_client = None
    if database == "mongodb":
        from pymongo import MongoClient
        shared_mongo_client = MongoClient(
            MONGO_URI,
            maxPoolSize=600,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=10000,
            appname="MongoCockroachBenchmark",
        )

    def mark_start() -> None:
        if sampler:
            sampler.start()
        start_time_holder["started"] = time.perf_counter()

    barrier = threading.Barrier(clients + 1, action=mark_start)

    all_latencies: list[float] = []
    total_success = 0
    all_errors: list[str] = []
    try:
        with ThreadPoolExecutor(max_workers=clients) as executor:
            futures = [
                executor.submit(
                    run_worker,
                    worker_id,
                    database,
                    record_count,
                    operations_per_client,
                    warmup_operations,
                    read_ratio,
                    barrier,
                    shared_mongo_client,
                )
                for worker_id in range(clients)
            ]
            # Release all prepared workers at nearly the same time.
            barrier.wait(timeout=120)
            for future in as_completed(futures):
                latencies, successful, errors = future.result()
                all_latencies.extend(latencies)
                total_success += successful
                all_errors.extend(errors)
    except Exception:
        if sampler and sampler.thread.is_alive():
            sampler.stop()
        if shared_mongo_client is not None:
            shared_mongo_client.close()
        raise

    if shared_mongo_client is not None:
        shared_mongo_client.close()
    started = start_time_holder.get("started", time.perf_counter())
    elapsed = max(time.perf_counter() - started, 1e-9)
    average_cpu = average_memory = None
    if sampler:
        average_cpu, average_memory = sampler.stop()

    expected_operations = clients * operations_per_client
    p95 = percentile(all_latencies, 95)
    average_latency = mean(all_latencies) if all_latencies else None
    throughput = total_success / elapsed
    status = "success" if total_success == expected_operations and not all_errors else "completed-with-errors"

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "database": database,
        "dataset_records": record_count,
        "workload": workload_name,
        "clients": clients,
        "operations_per_client": operations_per_client,
        "successful_operations": total_success,
        "errors": len(all_errors),
        "run_time_seconds": round(elapsed, 4),
        "p95_latency_ms": round(p95, 4) if p95 is not None else "",
        "average_latency_ms": round(average_latency, 4) if average_latency is not None else "",
        "throughput_ops_sec": round(throughput, 3),
        "average_cpu_percent": round(average_cpu, 3) if average_cpu is not None else "",
        "average_memory_mib": round(average_memory, 3) if average_memory is not None else "",
        "status": status,
        "errors_detail": all_errors[:5],
    }


def append_result(row: dict[str, Any]) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    new_file = not CSV_FILE.exists()
    with CSV_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in CSV_FIELDS})


def save_raw_log(row: dict[str, Any]) -> str:
    RAW.mkdir(parents=True, exist_ok=True)
    filename = (
        f"{row['database']}_{row['dataset_records']}_{row['workload']}_"
        f"{row['clients']}clients.json"
    )
    path = RAW / filename
    path.write_text(json.dumps(row, indent=2), encoding="utf-8")
    return str(path.relative_to(ROOT)).replace("\\", "/")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare MongoDB and CockroachDB performance.")
    parser.add_argument("--db", choices=["both", "mongodb", "cockroachdb"], default="both")
    parser.add_argument("--datasets", default="100000,1000000,10000000")
    parser.add_argument("--clients", default="1,10,50,100,500")
    parser.add_argument("--workloads", default="read-heavy,mixed,write-heavy")
    parser.add_argument("--operations-per-client", type=int, default=500)
    parser.add_argument("--warmup-operations", type=int, default=20)
    parser.add_argument("--quick", action="store_true", help="Small test: 10K records, 1/5 clients, 100 ops/client")
    parser.add_argument("--no-docker-stats", action="store_true", help="Skip CPU and RAM sampling")
    args = parser.parse_args()

    if args.quick:
        datasets = [10000]
        clients = [1, 5]
        workloads = ["read-heavy", "mixed", "write-heavy"]
        operations_per_client = 100
        warmup_operations = 10
    else:
        datasets = [int(value) for value in args.datasets.split(",") if value.strip()]
        clients = [int(value) for value in args.clients.split(",") if value.strip()]
        workloads = [value.strip() for value in args.workloads.split(",") if value.strip()]
        operations_per_client = args.operations_per_client
        warmup_operations = args.warmup_operations

    if any(value <= 0 for value in datasets + clients):
        parser.error("Dataset sizes and client counts must be positive.")
    if operations_per_client <= 0 or warmup_operations < 0:
        parser.error("Operations per client must be positive; warmup cannot be negative.")

    workloads_config = load_workloads()
    invalid_workloads = sorted(set(workloads) - set(workloads_config))
    if invalid_workloads:
        parser.error(f"Unknown workloads: {', '.join(invalid_workloads)}")

    if not shutil_which("docker"):
        raise RuntimeError("Docker CLI was not found. Install Docker Desktop and start it first.")

    MongoClient, psycopg = import_database_drivers()
    wait_for_databases()
    create_cockroach_database(psycopg)

    databases = ["mongodb", "cockroachdb"] if args.db == "both" else [args.db]
    print("\nBENCHMARK CONFIGURATION")
    print(f"Databases: {', '.join(databases)}")
    print(f"Dataset sizes: {datasets}")
    print(f"Workloads: {', '.join(workloads)}")
    print(f"Client counts: {clients}")
    print(f"Operations per client: {operations_per_client}")
    print("Workload mixes: read-heavy 95/5, mixed 50/50, write-heavy 5/95 (read/update)")
    print("Note: use --quick first to confirm your environment works.")

    for database in databases:
        for record_count in datasets:
            if database == "mongodb":
                reset_and_load_mongodb(MongoClient, record_count)
            else:
                reset_and_load_cockroach(psycopg, record_count)

            for workload_name in workloads:
                read_ratio = float(workloads_config[workload_name]["read"])
                for client_count in clients:
                    print(
                        f"\nTesting {database} | {record_count:,} records | "
                        f"{workload_name} | {client_count} clients"
                    )
                    row = run_benchmark(
                        database=database,
                        record_count=record_count,
                        workload_name=workload_name,
                        clients=client_count,
                        operations_per_client=operations_per_client,
                        warmup_operations=warmup_operations,
                        read_ratio=read_ratio,
                        collect_stats=not args.no_docker_stats,
                    )
                    row["raw_log"] = save_raw_log(row)
                    append_result(row)
                    print(
                        f"  Result: {row['status']} | throughput="
                        f"{row['throughput_ops_sec']} ops/s | p95="
                        f"{row['p95_latency_ms']} ms | CPU="
                        f"{row['average_cpu_percent']}% | RAM="
                        f"{row['average_memory_mib']} MiB | errors={row['errors']}"
                    )

    print(f"\nFinished. Results saved to: {CSV_FILE}")
    print("Create summary tables and charts with: python scripts/analyze_results.py")
    return 0


def shutil_which(command: str) -> Optional[str]:
    # Avoid importing shutil in the hot path; this is only used at startup.
    import shutil
    return shutil.which(command)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nBenchmark stopped by user.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
