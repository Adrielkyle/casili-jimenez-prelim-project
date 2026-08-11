import sqlite3
import random
import os

if os.path.exists("shop_oltp_p2.db"):
    os.remove("shop_oltp_p2.db")

conn = sqlite3.connect("shop_oltp_p2.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    container_id TEXT,
    shipping_line TEXT,
    port_name TEXT,
    transit_duration_hours REAL,
    order_status TEXT,
    order_date TEXT
)
""")

shipping_lines = ["Maersk", "MSC", "CMA CGM", "Hapag-Lloyd", "COSCO"]
ports = [" Manila ", "Singapore", " Rotterdam", "Los Angeles ", "Shanghai  "]

for i in range(1, 101):
    container_id = f"CONT{1000+i}" if random.random() > 0.1 else None
    shipping_line = random.choice(shipping_lines)
    port_name = random.choice(ports)
    duration = round(random.uniform(5, 300), 2)
    if random.random() < 0.1:
        duration = -abs(duration)  # inject corrupted negative value
    order_date = f"2026-0{random.randint(1,9)}-{random.randint(10,28)} 10:00:00"

    cursor.execute(
        "INSERT INTO orders (order_id, container_id, shipping_line, port_name, transit_duration_hours, order_status, order_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (i, container_id, shipping_line, port_name, duration, "ARRIVED", order_date)
    )

conn.commit()
conn.close()
print("Mock database created: shop_oltp_p2.db")