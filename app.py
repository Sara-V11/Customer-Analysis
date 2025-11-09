import streamlit as st
import pandas as pd
import os
import plotly.express as px

# --- Paths ---
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "features", "customer_segments.csv")

# --- Page config ---
st.set_page_config(page_title="Customer Segmentation Dashboard", layout="wide")
st.title("Customer Segmentation, CLV & Churn Dashboard")

# --- Load data ---
@st.cache_data
def load_data(path):
    df = pd.read_csv(path)
    # Compute churn risk dynamically if missing
    if "Churn_Risk" not in df.columns:
        df["Churn_Risk"] = df["RecencyDays"].apply(lambda x: "High" if x > 90 else "Low")
    return df

if not os.path.exists(DATA_PATH):
    st.error(f"Data not found at {DATA_PATH}. Run ETL first.")
    st.stop()

df = load_data(DATA_PATH)

# --- Precompute axis ranges for consistency ---
total_min, total_max = df["TotalSpend"].min(), df["TotalSpend"].max()
recency_min, recency_max = df["RecencyDays"].min(), df["RecencyDays"].max()
max_churn_count = df.groupby("Churn_Risk").size().max()

# --- Sidebar filters ---
st.sidebar.header("Filters")
cluster_options = ["All"] + sorted(df["Cluster"].unique().tolist())
cluster_choice = st.sidebar.selectbox("Select Cluster", cluster_options)

if cluster_choice == "All":
    filtered_df = df
else:
    filtered_df = df[df["Cluster"] == cluster_choice]

# --- Metrics ---
st.header("📊 Cluster Overview")
col1, col2, col3 = st.columns(3)
col1.metric("Customers in Selection", len(filtered_df))
col2.metric("Avg Total Spend", f"${filtered_df['TotalSpend'].mean():,.2f}")
col3.metric("Avg Recency (days)", round(filtered_df["RecencyDays"].mean(), 2))

# --- CLV / Total Spend Distribution ---
st.subheader("Customer Lifetime Value / Total Spend Distribution")
fig1 = px.histogram(
    filtered_df,
    x="TotalSpend",
    nbins=40,
    range_x=[total_min, total_max],  # fixed axis
    color="Cluster" if cluster_choice == "All" else None,
    title=f"Total Spend Distribution — {'All Clusters' if cluster_choice=='All' else f'Cluster {cluster_choice}'}"
)
st.plotly_chart(fig1, use_container_width=True)

# --- Churn Risk Breakdown ---
st.subheader("Churn Risk Breakdown")
# Ensure all categories are present
all_churn_categories = df["Churn_Risk"].unique()
churn_counts = (
    filtered_df.groupby("Churn_Risk").size()
    .reindex(all_churn_categories, fill_value=0)
    .reset_index(name="Count")
)

fig2 = px.bar(
    churn_counts,
    x="Churn_Risk",
    y="Count",
    color="Churn_Risk",
    title=f"Churn Risk — {'All Clusters' if cluster_choice=='All' else f'Cluster {cluster_choice}'}",
    color_discrete_map={"High": "red", "Low": "green"},
    range_y=[0, max_churn_count]  # fixed y-axis
)
st.plotly_chart(fig2, use_container_width=True)

# --- Spend vs Recency Scatter ---
st.subheader("Customer Segments (Spend vs Recency)")
fig3 = px.scatter(
    filtered_df,
    x="TotalSpend",
    y="RecencyDays",
    color="Cluster" if cluster_choice == "All" else None,
    title=f"Spend vs Recency — {'All Clusters' if cluster_choice=='All' else f'Cluster {cluster_choice}'}",
    hover_data=["CustomerID"],
    range_x=[total_min, total_max],     # fixed axis
    range_y=[recency_min, recency_max]  # fixed axis
)
st.plotly_chart(fig3, use_container_width=True)

st.success("Dashboard loaded")
