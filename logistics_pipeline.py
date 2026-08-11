import sqlite3
import pandas as pd
import logging
import os
import shutil


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


if __name__ == "__main__":
    setup_logging()
    df = extract_data("shop_oltp_p2.db")

    print(df.head())
    print(f"\nTotal rows: {len(df)}")
    print(f"Nulls in container_id: {df['container_id'].isnull().sum()}")
    print(f"Negative durations: {(df['transit_duration_hours'] < 0).sum()}")