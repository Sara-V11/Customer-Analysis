import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os
import psycopg2
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

# --- Logging setup ---

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("customer_segments")

# --- File paths ---

feature_file = os.path.join("data", "processed", "features", "customer_features.csv")
output_csv = os.path.join("data", "processed", "features", "customer_segments.csv")
elbow_plot_file = os.path.join("analysis", "outputs", "elbow_curve.png")
segment_plot_file = os.path.join("analysis", "outputs", "customer_segments.png")
os.makedirs(os.path.dirname(elbow_plot_file), exist_ok=True)
os.makedirs(os.path.dirname(segment_plot_file), exist_ok=True)

# --- Load features ---

logger.info(f"Loading customer features from {feature_file}...")
df = pd.read_csv(feature_file)
logger.info(f"Data shape: {df.shape}")
logger.info("First few rows:\n" + str(df.head()))

# --- Prepare features ---

X = df.drop("CustomerID", axis=1)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# --- Elbow method ---
inertia = []
K_range = range(2, 10)
for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(X_scaled)
    inertia.append(kmeans.inertia_)


plt.figure(figsize=(8, 5))
plt.plot(K_range, inertia, 'bo-')
plt.title("Elbow Method for Optimal K")
plt.xlabel("Number of clusters (k)")
plt.ylabel("Inertia")
plt.savefig(elbow_plot_file)
plt.close()
logger.info(f"Elbow curve saved to {elbow_plot_file}")

# K means clustering
k = 4  
kmeans = KMeans(n_clusters=k, random_state=42)
df["Cluster"] = kmeans.fit_predict(X_scaled)
df.to_csv(output_csv, index=False)
logger.info(f"Customer segments saved to {output_csv}")

# --- Cluster visualization ---

plt.figure(figsize=(8, 6))
sns.scatterplot(x="TotalSpend", y="RecencyDays", hue="Cluster", data=df, palette="tab10")
plt.title("Customer Segments by Spend vs Recency")
plt.savefig(segment_plot_file)
plt.close()
logger.info(f"Customer segments plot saved to {segment_plot_file}")

# --- PostgreSQL load ---

try:
    import psycopg2
    from config.config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_customer_clusters (
            CustomerID BIGINT PRIMARY KEY,
            TotalSpend FLOAT,
            NumOrders INT,
            AvgOrderValue FLOAT,
            RecencyDays INT,
            Cluster INT
        )
    """)
    conn.commit()

    # Insert data
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO dim_customer_clusters (CustomerID, TotalSpend, NumOrders, AvgOrderValue, RecencyDays, Cluster)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (CustomerID) DO UPDATE
            SET TotalSpend = EXCLUDED.TotalSpend,
                NumOrders = EXCLUDED.NumOrders,
                AvgOrderValue = EXCLUDED.AvgOrderValue,
                RecencyDays = EXCLUDED.RecencyDays,
                Cluster = EXCLUDED.Cluster
        """, (int(row.CustomerID), float(row.TotalSpend), int(row.NumOrders), float(row.AvgOrderValue), int(row.RecencyDays), int(row.Cluster)))
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Customer clusters saved to PostgreSQL successfully.")

except Exception as e:
    logging.error(f"Error saving clusters to PostgreSQL: {e}")
