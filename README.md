
# Customer Analysis & Segmentation Dashboard

**Early Insights into Customer Behavior, Lifetime Value (CLV), and Churn Risk**

This project implements a **complete ETL + analysis + dashboard pipeline** for customer segmentation. It allows businesses to explore customer clusters, track spending patterns, estimate CLV, and monitor churn risk using historical transaction data.

---

## **Live Demo**

Access the deployed Streamlit dashboard here:

[**Customer Segmentation Dashboard**](https://share.streamlit.io/Sara-V11/Customer-Analysis/main/app.py)

Features include:

* Interactive cluster selection
* Customer Lifetime Value / Total Spend distribution
* Churn risk breakdown per cluster
* Spend vs Recency scatterplots

---

## **Project Features**

* **Data ETL and Transformation**: Aggregate customer-level features from raw transactional data.
* **Customer Segmentation**: K-Means clustering with dynamic visualizations.
* **Churn Risk Estimation**: Simple rule-based labeling or model-based predictions.
* **Interactive Dashboard**: Built with Streamlit + Plotly for rich, interactive charts.
* **Predictive Modeling**: Train Random Forest models to predict high-risk churn customers.

---

## **Installation & Local Setup**

1. **Clone the repository**

```bash
git clone https://github.com/Sara-V11/Customer-Analysis.git
cd Customer-Analysis
```

2. **Create a virtual environment**

```bash
python -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Run ETL & segmentation**

```bash
python main.py
```

5. **Export summary metrics**

```bash
python analysis/export_summary.py
```

6. **Run the dashboard**

```bash
streamlit run app.py
```

---

## **Deployment on Streamlit Cloud**

1. Push your repo to GitHub.
2. Go to [Streamlit Cloud](https://share.streamlit.io/) and log in.
3. Click **New App → Select Repository → Branch (main)**.
4. Set **main file path** to `app.py`.
5. Click **Deploy**.

---

## **Dependencies**

* Python 3.13+
* pandas
* streamlit
* plotly
* scikit-learn
* joblib

---

## **License**

This project is **MIT Licensed** – feel free to use and modify for personal or educational purposes.

---
