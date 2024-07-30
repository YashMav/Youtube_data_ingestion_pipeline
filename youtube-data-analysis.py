import requests
from googleapiclient.discovery import build
import datetime
import re
import pandas as pd
import sqlalchemy as sal
from creds import api_key, oracle_db_username, oracle_db_pass
import oracledb

# create engine for oracle database
engine = sal.create_engine(f"oracle+oracledb://{oracle_db_username}:{oracle_db_pass}@localhost/?service_name=orclpdb")
# create connection
conn = engine.connect()

pd.set_option('display.min_rows', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# compile the regular expression for hours , minutes and seconds
hours_pattern = re.compile(r'(\d+)H')
minutes_pattern = re.compile(r'(\d+)M')
seconds_pattern = re.compile(r'(\d+)S')

# playlist_id = 'PLsyeobzWxl7poL9JTVyndKe62ieoN-MZ3'
playlist_id='PLgfig6lZhX7Sow5U2kQoOt0xNF_pW-kuH'
# creating service for YouTube
# build(serviceName , version , developerKey)

youtube = build('youtube', 'v3', developerKey=api_key)

# creating an empty dataframe with columns
df = pd.DataFrame(columns=['playlist_id', 'video_id', 'video_name', 'video_duration'])

nextPageToken = None
while True:

    pl_items = youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=playlist_id,
        pageToken=nextPageToken
    )

    pl_items_response = pl_items.execute()
    videos_list = []
    for vidid in pl_items_response['items']:
        video_id = vidid['contentDetails']['videoId']
        videos_list.append(video_id)
    # print(f"Videos = {videos_list}")
    video_details = youtube.videos().list(
        part='snippet,contentDetails',
        id=videos_list
    )
    video_details_response = video_details.execute()
    # print(video_details_response)
    for item in video_details_response['items']:
        video_id = item['id']
        video_title = item['snippet']['title']
        duration = item['contentDetails']['duration']

        hours = hours_pattern.search(duration)
        hours = int(hours.group(1)) if hours else 0
        minutes = minutes_pattern.search(duration)
        minutes = int(minutes.group(1)) if minutes else 0
        seconds = seconds_pattern.search(duration)
        seconds = int(seconds.group(1)) if seconds else 0

        video_seconds = int(datetime.timedelta(
            hours=hours,
            minutes=minutes,
            seconds=seconds
        ).total_seconds())

        data = {'playlist_id': playlist_id,
                'video_id': video_id, 'video_name': video_title, 'video_duration': video_seconds}

        df.loc[len(df)] = data
    nextPageToken = pl_items_response.get('nextPageToken')
    if not nextPageToken:
        break
print(df)

# STG table PLAYLIST_STG with respective metadata already created in Oracle Database

# truncate the stg table
query = sal.text('TRUNCATE TABLE PLAYLIST_STG')
conn.execute(query)

df.to_sql('playlist_stg', index=False, con=conn, if_exists='append')
conn.commit()
conn.close()
