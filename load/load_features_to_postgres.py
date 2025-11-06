import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql

# --- Add project root to sys.path ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config import *
from utils.logger import get_logger

logger = get_logger("load_features_to_postgres")

# --- File paths ---
features_file = os.path.join(project_root, "data", "processed", "features", "customer_features.csv")
table_name = "dim_customer_features"

# --- Load CSV ---
logger.info(f"Loading features from {features_file}...")
df = pd.read_csv(features_file)
logger.info(f"Loaded {len(df)} rows.")

# --- Connect to PostgreSQL ---
try:
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        dbname=REDSHIFT_DB,
        port=REDSHIFT_PORT
    )
    cur = conn.cursor()
    logger.info("Connected to PostgreSQL successfully.")
except Exception as e:
    logger.error(f"Error connecting to PostgreSQL: {e}")
    raise e

# --- Create table if not exists ---
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

# --- Insert data into PostgreSQL ---
logger.info("Inserting rows into PostgreSQL...")
for _, row in df.iterrows():
    insert_sql = f"""
    INSERT INTO {table_name} (CustomerID, TotalSpend, NumOrders, AvgOrderValue, RecencyDays)
    VALUES (%s, %s, %s, %s, %s)
    """
    cur.execute(insert_sql, (
        row.CustomerID,
        row.TotalSpend,
        row.NumOrders,
        row.AvgOrderValue,
        row.RecencyDays
    ))

conn.commit()
cur.close()
conn.close()
logger.info(f"Inserted {len(df)} rows into {table_name}.")
logger.info("Load complete!")
