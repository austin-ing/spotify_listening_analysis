import pandas as pd
import glob
import json

# Path to your Spotify data folder
DATA_PATH = "/Users/austining/Desktop/Spotify Data/Spotify Extended Streaming History/"

# Grab all JSON files
json_files = glob.glob(DATA_PATH + "*.json")

dfs = []

for file in json_files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
        df = pd.DataFrame(data)
        dfs.append(df)

# Combine into one DataFrame
spotify_combined = pd.concat(dfs, ignore_index=True)

# Write to a single JSON file
spotify_combined.to_json(
    "data/processed/spotify_combined_raw.json",
    orient="records",
    indent=2
)