# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
songplay_table_create = ("""CREATE TABLE songplays (
songplay_id SERIAL PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level text NOT NULL, 
song_id text NOT NULL, 
artist_id text NOT NULL, 
session_id text, 
location text, 
user_agent text,
song text, 
artist text,
length numeric)
""")

user_table_create = ("""CREATE TABLE users(
user_id int PRIMARY KEY, 
first_name text NOT NULL, 
last_name text NOT NULL, 
gender text CHECK(gender IN ('M', 'F')),
level text NOT NULL)
""")

song_table_create = ("""CREATE TABLE songs (
song_id text PRIMARY KEY , 
title text NOT NULL,
artist_id text, 
year numeric, 
duration numeric )
""")

artist_table_create = ("""CREATE TABLE artists(
artist_id text PRIMARY KEY, 
name text NOT NULL,  
location text NOT NULL, 
lattitude numeric, 
longitude numeric)
""")

time_table_create = ("""CREATE TABLE time(
start_time time PRIMARY KEY, 
hour int NOT NULL,  
day int NOT NULL,  
week int NOT NULL,  
month int NOT NULL,  
year int NOT NULL,  
weekday int NOT NULL)
""")

# INSERT RECORDS
songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent,song,artist,length)
values(%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s,%s ,%s ,%s);
""")

user_table_insert = (""" INSERT INTO users(user_id,first_name,last_name,gender,level)
values(%s, %s ,%s ,%s ,%s)
ON CONFLICT (user_id)
DO
    UPDATE
    SET level=EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration) 
values(%s, %s ,%s ,%s ,%s)
ON CONFLICT ON CONSTRAINT songs_pkey 
DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location,lattitude,longitude)
values(%s, %s ,%s ,%s ,%s)
ON CONFLICT ON CONSTRAINT artists_pkey 
DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year,weekday)
values(%s, %s ,%s ,%s ,%s,%s ,%s)
ON CONFLICT ON CONSTRAINT time_pkey
DO NOTHING;
""")

# FIND SONGS
song_select = ("""
Select distinct sng.song_id,sng.artist_id
FROM songs sng 
inner join artists art 
on sng.artist_id=art.artist_id 
WHERE sng.title = %s
or art.name = %s
or sng.duration = %s
""")


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]