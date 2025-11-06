import os
import sys
import logging
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from scripts import transform_features  

# --- Project root setup ---
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Logger setup ---
def get_logger(name="etl_pipeline"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)
    return logger

logger = get_logger()

# --- Configuration (change for Redshift later) ---
REDSHIFT_HOST = "localhost"
REDSHIFT_PORT = 5433
REDSHIFT_DB = "market_analysis"
REDSHIFT_USER = "amitv"
REDSHIFT_PASSWORD = "hihello@24"

# --- Paths ---
RAW_FILE = os.path.join(project_root, "data", "raw", "ecommerce_data.csv")
FEATURE_FILE = os.path.join(project_root, "data", "processed", "features", "customer_features.csv")
SEGMENT_FILE = os.path.join(project_root, "data", "processed", "features", "customer_segments.csv")
ELBOW_PLOT = os.path.join(project_root, "analysis", "outputs", "elbow_curve.png")
SEGMENT_PLOT = os.path.join(project_root, "analysis", "outputs", "customer_segments.png")

os.makedirs(os.path.dirname(ELBOW_PLOT), exist_ok=True)
os.makedirs(os.path.dirname(SEGMENT_PLOT), exist_ok=True)


# --- ETL Functions ---
def transform_data():
    logger.info("Step 1: Data transformation")
    try:
        df_features = transform_features.create_customer_features(
            RAW_FILE,  # path to your input CSV
            FEATURE_FILE  # path to save processed features
        )
        logger.info("Data transformation completed successfully.")
        return df_features
    except Exception as e:
        logger.error(f"Error in data transformation: {e}")
        raise e

def load_to_db(df, table_name):
    logger.info(f"Step 2: Load data into PostgreSQL table {table_name}")
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        cur = conn.cursor()

        # --- Create table dynamically ---
        columns = ", ".join([
            f"{col} FLOAT" if df[col].dtype != "int64" else f"{col} INT"
            for col in df.columns
        ])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});")
        conn.commit()

        cur.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()
        logger.info(f"Cleared existing records in table {table_name} before insert.")

        # --- Insert rows ---
        for _, row in df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            cur.execute(
                f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})",
                tuple(row)
            )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Inserted {len(df)} rows into {table_name}")

    except Exception as e:
        logger.error(f"Error loading to DB: {e}")
        raise e

def customer_segmentation(df, max_k=10):
    logger.info("Step 3: Customer segmentation")
    try:
        X = df.drop("CustomerID", axis=1)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # --- Elbow Method ---
        inertia = []
        K_range = range(2, max_k)
        for k in K_range:
            km = KMeans(n_clusters=k, random_state=42)
            km.fit(X_scaled)
            inertia.append(km.inertia_)

        plt.figure(figsize=(8, 5))
        plt.plot(K_range, inertia, 'bo-')
        plt.title("Elbow Method for Optimal K")
        plt.xlabel("Number of clusters (k)")
        plt.ylabel("Inertia")
        plt.savefig(ELBOW_PLOT)
        plt.close()
        logger.info(f"Elbow curve saved to {ELBOW_PLOT}")

        # --- Apply KMeans ---
        optimal_k = 4
        kmeans = KMeans(n_clusters=optimal_k, random_state=42)
        df["Cluster"] = kmeans.fit_predict(X_scaled)

        os.makedirs(os.path.dirname(SEGMENT_FILE), exist_ok=True)
        df.to_csv(SEGMENT_FILE, index=False)
        logger.info(f"Customer segments saved to {SEGMENT_FILE}")

        # --- Scatterplot visualization ---
        plt.figure(figsize=(8, 6))
        sns.scatterplot(x="TotalSpend", y="RecencyDays", hue="Cluster", data=df, palette="tab10")
        plt.title("Customer Segments by Spend vs Recency")
        plt.savefig(SEGMENT_PLOT)
        plt.close()
        logger.info(f"Customer segments plot saved to {SEGMENT_PLOT}")

        # --- Save clusters to DB ---
        load_to_db(df, "dim_customer_clusters")
        logger.info("Customer clusters saved to PostgreSQL successfully!")
        return df

    except Exception as e:
        logger.error(f"Error in customer segmentation: {e}")
        raise e


# --- Main Pipeline ---
if __name__ == "__main__":
    try:
        df_features = transform_data()
        load_to_db(df_features, "dim_customer_features")
        customer_segmentation(df_features)
        logger.info("✅ Full ETL + customer segmentation pipeline finished successfully!")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
