# DataOps Report
## Global Supply Chain & Logistics Delay Tracker

## 1. Purpose

This report defends the architectural choices made in the logistics ETL
pipeline against the core system reliability pillars covered in class
(Kleppmann): fault isolation, idempotency, and observability.

## 2. OLTP vs. OLAP Separation

`shop_oltp_p2.db` represents a transactional (OLTP) source: it is optimized
for recording individual shipping events one row at a time and must remain
available and uncorrupted for operational use. Running analytical queries
directly against it would risk locking contention and performance
degradation on the live system.

To avoid this, the pipeline **extracts** data out of the OLTP source into a
separate analytical (OLAP) layer — the Parquet files in `data/analytics/` —
so downstream reporting and aggregation (e.g. total delays per shipping
line) never touches the production database directly.

## 3. Fault Isolation & Quarantine Strategy

Two anomaly types are known to occur in the raw source, at roughly a 10%
corruption rate:

- `container_id` recorded as NULL (device/system sync failure)
- `transit_duration_hours` recorded as negative (timezone sync error)

Rather than letting these anomalies crash the pipeline or silently dropping
them from memory, `quarantine_split()` isolates them into
`quarantine/negative_durations.csv`, tagged with a `quarantine_reason`
column identifying which rule(s) the row violated. This means:

- The main pipeline run never halts on bad data
- Every quarantined row is auditable — nothing is silently discarded
- A single row can carry multiple violation reasons at once (e.g. both a
  NULL `container_id` and a negative duration), which the reason-tagging
  logic captures explicitly

## 4. PII Masking & Sanitization

`container_id` is treated as a sensitive tracking identifier and is
irreversibly masked using `hashlib.sha256()` before it reaches the
analytical layer, converting it into a fixed-length anonymous token. This
follows the same cryptographic approach specified for other project tracks
handling PII (e.g. patient IDs, card numbers), applied here to shipping
container identifiers.

`port_name` values are also standardized by stripping leading/trailing
whitespace (`.str.strip()`) to prevent the same physical port from being
treated as multiple distinct values downstream (e.g. `"Manila"` vs.
`" Manila "`).

## 5. Storage Serialization & Partitioning

Clean data is serialized as **Apache Parquet** rather than CSV/JSON, using
`pyarrow`. Parquet's columnar, compressed binary format preserves data
types on read-back (unlike CSV, which loses type information) and is
significantly more efficient for analytical queries at scale.

The output is partitioned Hive-style by `shipping_line`
(`shipping_line=<value>/`), directly supporting the business requirement to
aggregate total transit delays grouped per shipping line — queries can scan
only the relevant partition instead of the full dataset.

## 6. Idempotency

The orchestrator can be re-run against the same raw dataset any number of
times without producing duplicate rows or files. This is enforced by
resetting both output directories immediately before they are (re)written:

```python
def reset_output_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)
```

This function is called at the start of both `quarantine_split()` (before
writing `quarantine/negative_durations.csv`) and
`write_partitioned_parquet()` (before writing to `data/analytics/`). Rather
than relying solely on `pyarrow`'s dataset-merge behavior — which can leave
stale partitions behind if partition values change between runs — a full
directory reset guarantees that each run's output directory state reflects
only the current run, with no accumulation of duplicate or orphaned files.

## 7. Observability

Every stage of the pipeline logs structured, timestamped events to
`logs/transit_runs.log` via Python's built-in `logging` module — extraction
start/success/failure, quarantine counts, cleaning/hashing completion, and
final Parquet row counts. This gives a complete execution trace for
debugging and for verifying idempotent behavior across repeated runs.
