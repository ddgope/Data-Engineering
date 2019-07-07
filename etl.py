import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath):
    """ 
    process_song_file: This function will read the song file one by one,after processing , records wil go to song and artist dimension tables.  
     
    Parameters: 
    cur: cursor
    filepath: this contains the JSON filename with path.  
 
    """
    # open song file
    df = pd.read_json(filepath ,lines=True)

    # dropping duplicate values 
    df.drop_duplicates() 

    # insert song record
    song_data = list(df[['song_id', 'title','artist_id', 'year', 'duration']].iloc[0])
    song_data[3]=song_data[3].astype(float)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']].iloc[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """ 
    process_log_file: This function will read the log file one by one,
    1. Filter the records for NextSong
    2. Convert timestamp column to datetime and insert into time dimension table 
    3. load the users table
    4. Before inserting songplay dimension table, find the song_id and artist_id based on song title, artist name, and song duration time match otherwise it will insert none for song_id and artist_id.
     
    Parameters: 
    cur: cursor
    filepath: this contains the JSON filename with path.  
 
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']
    
    # convert timestamp column to datetime  
    df['ts']=pd.to_datetime(df['ts'])
    t=pd.DataFrame()
    t['ts']=df['ts']
    t['timestamp'] = t['ts'].dt.time
    t['hour'] = t['ts'].dt.hour
    t['day'] = t['ts'].dt.day
    t['week of year'] = t['ts'].dt.week
    t['month'] = t['ts'].dt.month
    t['year'] = t['ts'].dt.year
    t['weekday'] = t['ts'].dt.dayofweek 
    t.set_index('ts',inplace=True)
    
    # insert time data records   
    time_data = t.values.tolist() 
    column_labels = ('timestamp','hour','day','week of year','month','year','weekday')
    time_df = pd.DataFrame(time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df,columns=['userId','firstName','lastName','gender','level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length)) #
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = 'NULL', 'NULL'
       
        
    df['songid']=songid
    df['artistid']=artistid
    # insert songplay record
    songplay_data = pd.DataFrame(df,columns=['ts','userId','level','songid','artistid','sessionId','location','userAgent','song','artist','length'])
    for i, row in songplay_data.iterrows():
        cur.execute(songplay_table_insert, row)       

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()