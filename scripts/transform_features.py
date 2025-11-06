import pandas as pd
import logging
from datetime import datetime

def create_customer_features(raw_data_path, output_path):
    """
    Reads the raw dataset, aggregates by CustomerID,
    and saves processed customer features.
    """
    logging.info(f"Loading raw dataset from {raw_data_path}...")
    df = pd.read_csv(raw_data_path, encoding='latin1')

    logging.info(f"Loaded {len(df)} rows and {len(df.columns)} columns.")
    logging.info(f"Columns found: {list(df.columns)}")

    # Ensure correct columns exist
    expected_cols = ['InvoiceNo', 'StockCode', 'Description', 'Quantity',
                     'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
    if not all(col in df.columns for col in expected_cols):
        raise ValueError(f"Dataset missing expected columns: {expected_cols}")

    # Drop missing CustomerIDs
    df = df.dropna(subset=['CustomerID'])

    # Convert date
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    # Compute total amount
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']

    logging.info("Creating customer-level aggregations...")
    customer_features = df.groupby('CustomerID').agg({
        'TotalAmount': 'sum',
        'InvoiceNo': 'nunique',
    }).rename(columns={'TotalAmount': 'TotalSpend', 'InvoiceNo': 'NumOrders'}).reset_index()

    customer_features['AvgOrderValue'] = (
        customer_features['TotalSpend'] / customer_features['NumOrders']
    )

    logging.info("Calculating recency (days since last purchase)...")
    last_date = df['InvoiceDate'].max()
    recency = df.groupby('CustomerID')['InvoiceDate'].max().reset_index()
    recency['RecencyDays'] = (last_date - recency['InvoiceDate']).dt.days
    customer_features = pd.merge(customer_features, recency[['CustomerID', 'RecencyDays']], on='CustomerID')

    customer_features.to_csv(output_path, index=False)
    logging.info(f"Customer features saved: {len(customer_features)} rows at {output_path}")
    logging.info("First 5 rows:")
    logging.info(f"\n{customer_features.head()}")

    return customer_features
