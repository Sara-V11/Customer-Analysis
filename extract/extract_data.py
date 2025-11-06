import pandas as pd

# Step 1: Read dataset
df = pd.read_csv("data/raw/ecommerce_data.csv", encoding="ISO-8859-1")

# Step 2: Inspect basic info
print("Shape:", df.shape)
print(df.head())
print(df.info())
