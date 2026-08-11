import sqlite3
import hashlib
import pandas as pd
import logging
import os
import shutil
import pyarrow as pa
import pyarrow.parquet as pq


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename="logs/transit_runs.log",
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def extract_data(db_path: str) -> pd.DataFrame:
    logging.info(f"Starting extraction from {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM orders", conn)
        conn.close()
        logging.info(f"Extraction successful: {len(df)} rows pulled")
        return df
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise


def reset_output_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
        logging.info(f"Cleared existing output at {path}")
    os.makedirs(path, exist_ok=True)


def quarantine_split(df: pd.DataFrame, quarantine_dir="quarantine") -> pd.DataFrame:
    """
    Splits raw rows into clean vs quarantined.
    Quarantine reasons: NULL container_id, negative transit_duration_hours.
    Directory is reset first each run so quarantine/negative_durations.csv
    never accumulates stale rows across reruns (idempotency).
    """
    reset_output_dir(quarantine_dir)

    null_container = df["container_id"].isna()
    negative_duration = df["transit_duration_hours"] < 0
    is_bad = null_container | negative_duration

    bad_rows = df[is_bad].copy()
    clean_rows = df[~is_bad].copy()

    def reason(row):
        reasons = []
        if pd.isna(row["container_id"]):
            reasons.append("null_container_id")
        if row["transit_duration_hours"] < 0:
            reasons.append("negative_transit_duration")
        return ";".join(reasons)

    if not bad_rows.empty:
        bad_rows["quarantine_reason"] = bad_rows.apply(reason, axis=1)

    quarantine_path = os.path.join(quarantine_dir, "negative_durations.csv")
    bad_rows.to_csv(quarantine_path, index=False)

    logging.info(f"Quarantined {len(bad_rows)} rows -> {quarantine_path}")
    logging.info(f"Clean rows remaining: {len(clean_rows)}")

    return clean_rows


def clean_and_hash(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strips whitespace/casing issues on port_name and cryptographically
    masks container_id using SHA-256.
    """
    df = df.copy()
    df["port_name"] = df["port_name"].str.strip()
    df["container_id"] = df["container_id"].apply(
        lambda x: hashlib.sha256(str(x).encode()).hexdigest()
    )
    logging.info("Cleaned port_name whitespace and hashed container_id (SHA-256)")
    return df


def write_partitioned_parquet(df: pd.DataFrame, output_dir="data/analytics"):
    """
    Writes clean data as Hive-style Parquet partitioned by shipping_line.
    output_dir is reset first each run so reruns never leave duplicate
    or stale partition files behind (idempotency).
    """
    reset_output_dir(output_dir)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=["shipping_line"],
    )
    logging.info(f"Wrote {len(df)} clean rows to {output_dir}, partitioned by shipping_line")


if __name__ == "__main__":
    setup_logging()
    df = extract_data("shop_oltp_p2.db")

    print(df.head())
    print(f"\nTotal rows: {len(df)}")
    print(f"Nulls in container_id: {df['container_id'].isnull().sum()}")
    print(f"Negative durations: {(df['transit_duration_hours'] < 0).sum()}")

    clean_df = quarantine_split(df)
    clean_df = clean_and_hash(clean_df)
    write_partitioned_parquet(clean_df)

    print(f"\nFinal clean rows written to Parquet: {len(clean_df)}")
