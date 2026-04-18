import pandas as pd

df = pd.read_excel("./data/raw/data.xls")

print("Shape:", df.shape)
print("\nColumns:", df.columns.tolist())

print("\nInfo:")
print(df.info())

print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicates:", df.duplicated().sum())

print("\nDescribe:")
print(df.describe())