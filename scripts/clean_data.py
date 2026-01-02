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

# def get_date_song_listened(ts):
#     date = ts[0:10]
#     return date

# def get_time_song_listened(ts):
#     hour = ts[11:13]
#     minute = ts[15:17]
#     seconds = ts[19:21]
#     hour_int = int(hour)

#     if hour_int == 0:
#         hour_int = 12
#         return f'{hour_int}:{minute}:{seconds}AM'
#     elif hour_int > 0 and hour_int < 12:
#         return f'{hour_int}:{minute}:{seconds}AM'
#     elif hour_int == 12:
#         return f'{hour_int}:{minute}:{seconds}PM'
#     else:
#         hour_int = hour_int - 12
#         return f'{hour_int}:{minute}:{seconds}PM'
    

def clean_spotify_data(raw_file, clean_file_csv, clean_file_json):

    df = pd.read_json(RAW_FILE)
    # write cleaning code

    # drop columns: ip_addr, episode_name, episode_show_name, sptify_episode_uri, audiobook_title, audiobook_uri, audiobook_chapter_uri, audiobook_chapter_title

    columns_drop = ['ip_addr', 'episode_name', 'episode_show_name', 'spotify_episode_uri', 'audiobook_title', 'audiobook_uri', 'audiobook_chapter_uri', 'audiobook_chapter_title']

    df.drop(columns_drop, axis=1, inplace=True)

    # convert ms_played from milliseconds to minutes for analysis

    df['ms_played'] = df['ms_played'].apply(lambda x: convert_to_minutes(x))

    # extract ts into month, day, year and hour, minutes, seconds
    # add each col, the drop ts column

    # df.insert(0,'date_listened')
    # df['date_listened'] = df['ts'].apply(lambda x: get_date_song_listened(x))

    # df.insert(0, 'time_listened')
    # df['time_listened'] = df['ts'].apply(lambda x: get_time_song_listened(x))

    df['ts'] = pd.to_datetime(df['ts'])

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

    # save csv
    df.to_csv(clean_file_csv, index=False)

    # save json (records + lines)
    df.to_json(clean_file_json, orient='records', lines=True)

    # display
    print(df.head())
    print(df.info())
    print(df.describe())

    return df

if __name__ == "__main__":
    clean_spotify_data(RAW_FILE, CLEAN_FILE_CSV, CLEAN_FILE_JSON)

