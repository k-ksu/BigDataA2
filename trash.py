import pandas as pd

df = pd.read_parquet("app/data/parquet/m.parquet")

print(df.columns)
print(df.head(3))
