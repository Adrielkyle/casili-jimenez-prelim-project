# Global Supply Chain & Logistics Delay Tracker

## Overview
This project is a production-grade, fault-tolerant data pipeline that ingests raw container
shipping records from a simulated transactional database (`shop_oltp_p2.db`), cleans and
validates the data, quarantines corrupted rows, cryptographically masks sensitive identifiers,
and writes the clean output as Hive-style partitioned Apache Parquet files for analytics.

## Business Case
A logistics firm tracks shipping containers globally via terminal checkpoints. The raw
tracking database contains operational anomalies such as negative transit durations
(caused by time-zone sync errors) and port names with messy leading/trailing whitespace.
This pipeline sanitizes the metadata, isolates temporal errors into a quarantine layer, and
prepares clean data aggregated per shipping line for downstream analytics.

## Pipeline Lineage / Flow Diagram

```
                 ┌─────────────────────────┐
                 │   shop_oltp_p2.db        │
                 │   (SQLite, table:orders) │
                 └────────────┬─────────────┘
                              │  extract_data()
                              ▼
                 ┌─────────────────────────┐
                 │  Raw DataFrame (df)      │
                 └────────────┬─────────────┘
                              │  quarantine_split()
                ┌─────────────┴─────────────┐
                ▼                           ▼
    ┌────────────────────┐      ┌───────────────────────────┐
    │ quarantine/         │      │  Clean rows (in-memory)   │
    │ negative_durations  │      │                            │
    │ .csv                │      │                            │
    │ (null container_id, │      └─────────────┬──────────────┘
    │ negative duration)  │                    │  clean_and_hash()
    └────────────────────┘                    ▼
                                   ┌───────────────────────────┐
                                   │ .strip() port_name         │
                                   │ SHA-256 hash container_id  │
                                   └─────────────┬───────────────┘
                                                 │  write_partitioned_parquet()
                                                 ▼
                                   ┌───────────────────────────┐
                                   │ data/analytics/             │
                                   │   shipping_line=X/          │
                                   │     part-0.parquet          │
                                   └───────────────────────────┘

Throughout the run:
  logging → logs/transit_runs.log (timestamped execution trace)
```

## Project Structure
```
casili-jimenez-prelim-project/
├── data/
│   └── analytics/              <- Final Hive-partitioned Parquet output (by shipping_line)
├── logs/
│   └── transit_runs.log        <- Automated execution trace log
├── quarantine/
│   └── negative_durations.csv  <- Isolated corrupted rows (null container_id / negative duration)
├── requirements.txt            <- Locked pip package dependencies
├── logistics_pipeline.py       <- Central executable pipeline script
├── create_mock_db.py           <- Script to generate the mock SQLite source database
├── shop_oltp_p2.db             <- Simulated OLTP source database
├── DataOps_Report.md           <- Technical write-up on reliability, idempotency, security
└── README.md                   <- This file
```

## How the Pipeline Works
1. **`setup_logging()`** — Initializes the `logs/` directory and configures timestamped logging.
2. **`extract_data()`** — Connects to `shop_oltp_p2.db` via `sqlite3` and pulls all rows from
   the `orders` table into a pandas DataFrame.
3. **`quarantine_split()`** — Flags and separates rows with a null `container_id` or a
   negative `transit_duration_hours`. Bad rows are written to
   `quarantine/negative_durations.csv` with a `quarantine_reason` column. The quarantine
   directory is reset each run to keep results idempotent.
4. **`clean_and_hash()`** — Strips whitespace from `port_name` and hashes `container_id`
   using SHA-256 to mask the identifier.
5. **`write_partitioned_parquet()`** — Writes the clean, hashed data as Hive-style
   partitioned Parquet files under `data/analytics/`, partitioned by `shipping_line`. The
   output directory is reset each run to prevent duplicate files on reruns.

## Setup & Execution

### 1. Clone the repository
```bash
git clone https://github.com/Adrielkyle/casili-jimenez-prelim-project.git
cd casili-jimenez-prelim-project
```

### 2. Create and activate a virtual environment
```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
source .venv/bin/activate   # macOS/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the pipeline
```bash
python logistics_pipeline.py
```

## Idempotency
Both the `quarantine/` and `data/analytics/` output directories are fully reset
(`reset_output_dir()`) at the start of every run. Running `logistics_pipeline.py` multiple
times against the same source database always produces identical directory states, with
no duplicated rows or leftover stale partitions.

## Output Verification
- Check `logs/transit_runs.log` for a timestamped trace of the run.
- Check `quarantine/negative_durations.csv` for isolated bad rows and their reasons.
- Check `data/analytics/shipping_line=<value>/` for the final partitioned Parquet files
  (viewable with the VS Code "Parquet Viewer" extension).
