import os 
import glob
import pandas as pd

from sql_queries import *
from create_table import *

def process_song_files(cur,file):
    df = pd.read_json(file,lines=True)

    df = df.where(pd.notnull(df), None)

    song_data = df[['song_id','title','artist_id','year','duration']].values[0]

    cur.execute(songs_insert_data,song_data[0],song_data[1],song_data[2],song_data[3],song_data[4])

    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0]

    cur.execute(artists_insert_data,artist_data[0],artist_data[1],artist_data[2],artist_data[3],artist_data[4])

    

def process_log_files(cur,file):
    df = pd.read_json(file,lines=True)
    
    df = df[df.page == "NextSong"]

    user_data = df[['userId','firstName','lastName','gender','level']].drop_duplicates().sort_values('userId')

    for index, row in user_data.iterrows():
        cur.execute(users_insert_data,row.userId, row.firstName, row.lastName, row.gender, row.level)
    
    df_time = df.copy()

    df_time['ts'] = pd.to_datetime(df_time.ts,unit='ms')

    ts = {
        "starttime" : df_time.ts, 
        "hour" : df_time.ts.dt.hour, 
        "day" : df_time.ts.dt.day, 
        "week" : df_time.ts.dt.dayofweek, 
        "month" : df_time.ts.dt.month, 
        "year" : df_time.ts.dt.year, 
        "weekday" : df_time.ts.dt.weekday
    }

    df_ts = pd.DataFrame.from_dict(ts)

    for index, row in df_ts.iterrows():
        cur.execute(time_insert_data,row.starttime, row.hour, row.day, row.week, row.month, row.year, row.weekday)

    for index, row in df.iterrows():
        result = cur.execute("select a.song_id, b.artist_id from songs a left join artists b on a.artist_id = b.artist_id where a.title = ? and b.name = ? and b.location = ?", row.song, row.artist, row.location).fetchone()
        songId, artistId = result if result else None, None

        cur.execute(songplays_insert_data,pd.to_datetime(row.ts,unit="ms"), row.userId, row.level, songId, artistId, row.sessionId, row.location, row.userAgent)

def get_files(cur, conn, dir, func):
    all_files = []
    for root, dirs, files in os.walk(dir):
        files = glob.glob(os.path.join(root,"*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print("{} files found in {}".format(num_files, dir))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed".format(i,num_files))

def main():
    cur, conn = connection('127.0.0.1', 'sparkifydb', 'sa', 'P@ssw0rd')

    get_files(cur, conn, 'data/song_data', process_song_files)
    get_files(cur, conn, 'data/log_data', process_log_files)

    conn.close()   


if __name__ == "__main__":
    main()