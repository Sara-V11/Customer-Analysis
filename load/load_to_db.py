import os
import sys

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD
from utils.logger import get_logger
import pandas as pd
import psycopg2

logger = get_logger("load_to_db")

# Load CSV
features_file = os.path.join(project_root, "data", "processed", "features", "customer_features.csv")
df = pd.read_csv(features_file)
logger.info(f"Loaded {len(df)} rows from {features_file}")

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cur = conn.cursor()
    logger.info("Connected to PostgreSQL successfully.")
except Exception as e:
    logger.error(f"Error connecting to PostgreSQL: {e}")
    raise e

# Create table if not exists
table_name = "dim_customer_features"
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    CustomerID FLOAT,
    TotalSpend FLOAT,
    NumOrders INT,
    AvgOrderValue FLOAT,
    RecencyDays INT
);
"""
cur.execute(create_table_sql)
conn.commit()
logger.info(f"Table {table_name} is ready.")

# Insert data
logger.info("Inserting rows into PostgreSQL...")
for _, row in df.iterrows():
    cur.execute(
        f"""
        INSERT INTO {table_name} (CustomerID, TotalSpend, NumOrders, AvgOrderValue, RecencyDays)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            float(row.CustomerID),
            float(row.TotalSpend),
            int(row.NumOrders),
            float(row.AvgOrderValue),
            int(row.RecencyDays),
        )
    )
conn.commit()
cur.close()
conn.close()
logger.info(f"Inserted {len(df)} rows into {table_name}.")
logger.info("Load complete!")

