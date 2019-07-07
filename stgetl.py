import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *


def process_song_file(cur, filepath,song_files):
    """ 
    process_song_file: This function will read the song file one by one,after processing , records wil go to song and artist dimension tables.  
     
    Parameters: 
    cur: cursor
    filepath: this contains the JSON filename with path.  
 
    """
   
    df=[]
    for f in song_files:
        df.append(pd.read_json(f ,lines=True))    
    
    # dropping duplicate values 
    #df.drop_duplicates() 

    # insert song record
    song_data = list(df[['song_id', 'title','artist_id', 'year', 'duration']].iloc[0])
    song_data[3]=song_data[3].astype(float)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude']].iloc[0])
    cur.execute(artist_table_insert, artist_data)



def process_data(cur, conn, filepath,all_files, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))   


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, all_files, func=process_song_file)
   

    conn.close()


if __name__ == "__main__":
    main()