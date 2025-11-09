import os
import pandas as pd

# Paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INPUT = os.path.join(PROJECT_ROOT, "data", "processed", "features", "customer_segments.csv")
SUMMARY_OUT = os.path.join(PROJECT_ROOT, "data", "processed", "summary_metrics.csv")
SEGMENT_OUT = os.path.join(PROJECT_ROOT, "data", "processed", "segment_summary.csv")

# Load data
df = pd.read_csv(INPUT)
if "Churn_Risk" not in df.columns:
    df["Churn_Risk"] = df["RecencyDays"].apply(lambda x: "High" if x > 90 else "Low")


# Build aggregation list
agg_list = [
    ("CustomerID", "count", "Num_Customers"),
    ("TotalSpend", "mean", "Avg_TotalSpend"),
    ("NumOrders", "mean", "Avg_NumOrders"),
    ("RecencyDays", "mean", "Avg_Recency")
]

# Optional columns
if "CLV_Estimate" in df.columns:
    agg_list.append(("CLV_Estimate", "mean", "Avg_CLV"))
if "Churn_Risk" in df.columns:
    agg_list.append(("Churn_Risk", lambda x: (x == "High").sum(), "Num_High_ChurnRisk"))

# Create cluster summary
cluster_summary = df.groupby("Cluster").agg(**{
    new_name: (col, func) for col, func, new_name in agg_list
}).reset_index()

# Segment-level summary (if column exists)
if "Segment" in df.columns:
    segment_summary = df.groupby("Segment").agg(
        Num_Customers=("CustomerID", "count"),
        Avg_CLV=("CLV_Estimate", "mean") if "CLV_Estimate" in df.columns else ("CustomerID", "count")
    ).reset_index()
else:
    segment_summary = pd.DataFrame()

# Save
os.makedirs(os.path.dirname(SUMMARY_OUT), exist_ok=True)
cluster_summary.to_csv(SUMMARY_OUT, index=False)
segment_summary.to_csv(SEGMENT_OUT, index=False)

print("Cluster summary saved to:", SUMMARY_OUT)
print(cluster_summary)
if not segment_summary.empty:
    print("\nSegment summary saved to:", SEGMENT_OUT)
    print(segment_summary)
