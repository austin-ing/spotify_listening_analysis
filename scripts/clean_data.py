
# Objective: “How do my listening habits change over time, and what patterns can I uncover in what, when, and how I listen to music?”

import pandas as pd

# RAW_FILE = "../data/raw/spotify_combined_raw.json"
# CLEAN_FILE = "../data/processed/spotify_cleaned.json"

RAW_FILE = "../spotify_json_backup/spotify_combined_raw.json"
CLEAN_FILE_JSON = "../spotify_json_backup/spotify_cleaned.json"
CLEAN_FILE_CSV = "../spotify_json_backup/spotify_cleaned.csv"

def convert_to_minutes(ms):
    ms_int = int(ms)
    minutes = ms_int / 60000
    return minutes
    
def convert_offline_timestamp_to_datetime(series):
    return pd.to_datetime(
        series.where(series < 1e11, series / 1000),
        unit='s',
        errors='coerce'
    )

# 1. Extract
def extract_spotify_data(raw_file):
    return pd.read_json(raw_file)

#2. Transform
def transform_spotify_data(df):
    # drop columns: ip_addr, episode_name, episode_show_name, sptify_episode_uri, audiobook_title, audiobook_uri, audiobook_chapter_uri, audiobook_chapter_title

    columns_drop = ['ip_addr', 'episode_name', 'episode_show_name', 'spotify_episode_uri', 'audiobook_title', 'audiobook_uri', 'audiobook_chapter_uri', 'audiobook_chapter_title']

    df.drop(columns_drop, axis=1, inplace=True)

    # convert ms_played from milliseconds to minutes for analysis

    df['ms_played'] = df['ms_played'].apply(lambda x: convert_to_minutes(x))

    # convert unix time to datetime
    df['offline_datetime'] = convert_offline_timestamp_to_datetime(df['offline_timestamp'])

    # extract ts into month, day, year and hour, minutes, seconds
    # add each col, the drop ts column

    df['ts'] = pd.to_datetime(df['ts'])

    df["offline_hour"] = df["offline_datetime"].dt.hour
    df["offline_day"] = df["offline_datetime"].dt.day_name()

    df.insert(0, 'year', df['ts'].dt.year)
    df.insert(1, 'month', df['ts'].dt.month)
    df.insert(2, 'day', df['ts'].dt.day)
    df.insert(3, 'day_of_week', df['ts'].dt.day_name())
    # 24 hour time for analysis
    df.insert(4, 'hour', df['ts'].dt.hour)
    df.insert(5, 'date', df['ts'].dt.date)
    # shows the 12 hour time
    df.insert(6, 'hour_12h', df['hour'].apply(lambda h: f"{(h % 12) or 12} {'AM' if h < 12 else 'PM'}"))

    df.drop('ts', axis=1, inplace=True)

    # rename columns to something actually usable

    df.rename(columns={
    'master_metadata_track_name': 'track_name',
    'master_metadata_album_artist_name': 'artist_name',
    'master_metadata_album_album_name': 'album_name',
    'spotify_track_uri': 'track_uri',
    'conn_country': 'country',
    'ms_played': 'minutes_played',
    'reason_start': 'start_reason',
    'reason_end': 'end_reason',
    'offline_timestamp': 'offline_ts'
    }, inplace=True)

    # drop old timestamp column
    df.drop(columns=['offline_ts'], inplace=True)

    return df

# 3. Load
def load_spotify_data(df, csv_path, json_path):
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient='records', lines=True)

# Run the Cleaner
def clean_spotify_data(raw_file, csv_path, json_path):
    df = extract_spotify_data(raw_file)
    df = transform_spotify_data(df)
    load_spotify_data(df, csv_path, json_path)

    print(df.head())
    print(df.info())
    print(df.describe())

    return df

if __name__ == "__main__":
    clean_spotify_data(
        RAW_FILE,
        CLEAN_FILE_CSV,
        CLEAN_FILE_JSON
    )


# Reference Data:
#      {
#     "ts":"2025-09-09T22:20:30Z", --> YYYY-MM-DDTHH:MM:SSZ  --> time song finished playing
#     "platform":"ios",
#     "ms_played":202400,
#     "conn_country":"US",
#     "ip_addr":"128.6.147.86",
#     "master_metadata_track_name":"AYAYAYA",
#     "master_metadata_album_artist_name":"IZ*ONE",
#     "master_metadata_album_album_name":"BLOOM*IZ",
#     "spotify_track_uri":"spotify:track:4XKCLNyCTvtdkLu5O1mzTU",
#     "episode_name":null,
#     "episode_show_name":null,
#     "spotify_episode_uri":null,
#     "audiobook_title":null,
#     "audiobook_uri":null,
#     "audiobook_chapter_uri":null,
#     "audiobook_chapter_title":null,
#     "reason_start":"trackdone",
#     "reason_end":"trackdone",
#     "shuffle":true,
#     "skipped":false,
#     "offline":false,
#     "offline_timestamp":1757456076.0,
#     "incognito_mode":false
#   }

