import pandas as pd
from config.config import load_config

config = load_config()
df = pd.read_excel(config["data"]["test_data"])

print("Shape:", df.shape)
print("\nColumns:", df.columns.tolist())

print("\nInfo:")
print(df.info())

print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicates:", df.duplicated().sum())

print("\nDescribe:")
print(df.describe())