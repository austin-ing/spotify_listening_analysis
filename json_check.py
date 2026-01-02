import pandas as pd

df = pd.read_json("data/processed/spotify_combined_raw.json")
print(df.head())
print(df.info())
print(df.columns)
