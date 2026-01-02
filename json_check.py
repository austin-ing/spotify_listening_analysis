import pandas as pd

RAW_FILE = "../spotify_json_backup/spotify_combined_raw.json"
df = pd.read_json(RAW_FILE)

print(df.head())
print(df.info())
print(df.columns)
