# Global Supply Chain & Logistics Delay Tracker
Prelim Mini Project — Data Warehousing (Lec/Lab)

## Overview
This project builds an end-to-end, fault-tolerant ETL pipeline that ingests raw
container shipping records from a simulated transactional database
(`shop_oltp_p2.db`), isolates data quality anomalies, sanitizes and masks
identifying fields, and writes clean analytical output as Hive-partitioned
Apache Parquet files.

## Pipeline Lineage

```
shop_oltp_p2.db (SQLite, table: orders)
        |
        v
[1] extract_data()
    - Connects via sqlite3, pulls all rows into a pandas DataFrame
    - Logs start/success/failure to logs/transit_runs.log
        |
        v
[2] quarantine_split()
    - Flags rows where container_id is NULL
    - Flags rows where transit_duration_hours < 0
    - Bad rows -> quarantine/negative_durations.csv (with quarantine_reason column)
    - Clean rows continue downstream
        |
        v
[3] clean_and_hash()
    - Strips whitespace from port_name
    - Hashes container_id with SHA-256 (hashlib) for anonymization
        |
        v
[4] write_partitioned_parquet()
    - Writes clean, sanitized data to data/analytics/
    - Hive-style partitioning: shipping_line=<value>/
    - Output directory reset before each write (idempotency)
```

## Repository Structure

```
casili-jimenez-prelim-project/
├── data/
│   └── analytics/                     <- Partitioned Parquet output (by shipping_line)
├── logs/
│   └── transit_runs.log               <- Execution trace log
├── quarantine/
│   └── negative_durations.csv         <- Isolated corrupted/anomalous rows
├── requirements.txt                   <- Locked pip dependencies
├── create_mock_db.py                  <- Generates the mock shop_oltp_p2.db source
├── logistics_pipeline.py              <- Main orchestrator script
├── DataOps_Report.md                  <- Reliability & idempotency write-up
└── README.md                          <- This file
```

## Setup & Execution

1. **Clone the repo and create a virtual environment**
   ```
   git clone https://github.com/Adrielkyle/casili-jimenez-prelim-project.git
   cd casili-jimenez-prelim-project
   python3 -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   ```

2. **Install locked dependencies**
   ```
   pip install -r requirements.txt
   ```

3. **(Optional) Regenerate the mock source database**
   ```
   python3 create_mock_db.py
   ```

4. **Run the pipeline**
   ```
   python3 logistics_pipeline.py
   ```

   This will:
   - Extract all rows from `shop_oltp_p2.db`
   - Quarantine anomalous rows to `quarantine/negative_durations.csv`
   - Clean and hash the remaining rows
   - Write partitioned Parquet output to `data/analytics/`
   - Log every step to `logs/transit_runs.log`

## Data Quality Rules

| Field                    | Anomaly                        | Handling                          |
|---------------------------|---------------------------------|------------------------------------|
| `container_id`            | NULL values                    | Quarantined; hashed (SHA-256) if clean |
| `transit_duration_hours`  | Negative values                | Quarantined                        |
| `port_name`                | Leading/trailing whitespace    | Stripped via `.str.strip()`        |

## Team

- Harvee D. Jimenez — Quarantine logic, data sanitization, SHA-256 masking, Parquet partitioning
- Kyle Casili — Extraction, logging, mock database generation
