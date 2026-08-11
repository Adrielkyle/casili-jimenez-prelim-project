# DataOps Report — Global Supply Chain & Logistics Delay Tracker

## 1. Purpose
This report defends the system reliability decisions made in `logistics_pipeline.py`,
based on Martin Kleppmann's core system pillars (Reliability, Scalability, Maintainability),
with a focus on **Idempotency** and **Fault Isolation**.

## 2. OLTP vs. OLAP Rationale
The source database (`shop_oltp_p2.db`) is a transactional (OLTP) system optimized for
fast, individual record writes from terminal checkpoints — not for large-scale aggregate
querying. Running analytical queries (e.g., "average transit delay per shipping line")
directly against a production OLTP system risks locking rows and degrading performance for
live operations. This pipeline extracts data out of the OLTP source and serializes it into
an OLAP-friendly, partitioned Parquet layer (`data/analytics/`) so downstream analytics can
run without ever touching the production database, preserving OLTP system reliability.

## 3. Fault Isolation & Quarantine Strategy
Raw data is deconstructed and validated in `quarantine_split()`. Two structural anomalies
are checked for every row:
- **Null `container_id`** — a missing structural identifier.
- **Negative `transit_duration_hours`** — a malformed value from time-zone sync errors.

Rows matching either condition are **not dropped from memory**. They are:
1. Tagged with a human-readable `quarantine_reason` (e.g., `null_container_id`,
   `negative_transit_duration`) so an engineer can triage them without re-deriving why they
   failed.
2. Written out to `quarantine/negative_durations.csv` as a permanent, inspectable record.

This ensures corrupted data never reaches the analytical layer, but nothing is silently
lost — satisfying the "Guided Response/Mechanism" objective of separating and archiving
corrupted source data without halting pipeline runtime execution. The pipeline does not
crash or exit on encountering bad rows; it isolates them and continues.

## 4. PII / Sensitive Data Masking
`container_id` values are cryptographically masked using SHA-256 (`hashlib.sha256`) in
`clean_and_hash()`, converting the identifier into a fixed-length, irreversible token before
it is written to the analytical layer. `port_name` strings are also standardized with
`.strip()` to remove inconsistent leading/trailing whitespace, preventing duplicate
group-by keys (e.g., `"Manila "` vs `"Manila"`) from fragmenting aggregation results.

## 5. Storage Serialization & Partitioning
Clean data is written using `pyarrow.parquet.write_to_dataset()` with
`partition_cols=["shipping_line"]`. This produces native **Hive-style** partitioning
(`shipping_line=<value>/part-*.parquet`), rather than a flat file. Storing data this way:
- Preserves column data types (unlike CSV/JSON, which lose type information on read-back).
- Enables partition pruning — downstream queries filtering by `shipping_line` can skip
  reading irrelevant partitions entirely, improving performance at scale.

## 6. Idempotency
Kleppmann identifies idempotency as critical for system reliability under retries and
reruns. Both output-producing functions in this pipeline enforce it directly:

- `quarantine_split()` calls `reset_output_dir("quarantine")` before writing.
- `write_partitioned_parquet()` calls `reset_output_dir("data/analytics")` before writing.

`reset_output_dir()` deletes the existing directory (`shutil.rmtree`) and recreates it
before any new files are written. As a result, running `logistics_pipeline.py` any number
of times against the same source database always produces the exact same final directory
state — no duplicate rows, no stale partition files, no accumulating quarantine records.

## 7. Observability
`setup_logging()` configures Python's built-in `logging` module to write timestamped,
leveled entries (`INFO` / `ERROR`) to `logs/transit_runs.log` for every pipeline run. Key
events logged include: extraction start/success/failure, quarantine row counts, and final
row counts written to Parquet. This creates a persistent, auditable execution trace for
every run, satisfying the DataOps observability requirement without requiring the console
output to be manually captured.

## 8. Summary
| Reliability Pillar | Implementation |
|---|---|
| Fault Isolation | `quarantine_split()` traps bad rows via boolean masks, never crashes on dirty data |
| Idempotency | `reset_output_dir()` clears `quarantine/` and `data/analytics/` before every write |
| Data Privacy | `container_id` masked via SHA-256 in `clean_and_hash()` |
| Observability | `logging` module writes structured trace to `logs/transit_runs.log` |
| Storage Integrity | Hive-partitioned Parquet via `pyarrow`, preserving schema and enabling partition pruning |
