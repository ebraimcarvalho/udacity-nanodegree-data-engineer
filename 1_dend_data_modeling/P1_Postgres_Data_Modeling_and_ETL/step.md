``` py
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def main() :
  """ Start connection and cursor for create database, drop and create table
  """
  cur, conn = create_database()

  drop_tables(cur, conn)
  create_tables(cur, conn)

  cur.close()
  conn.close()

if __name__ == "__main__":
  main()

def create_database():
  conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
  conn.set_session(autocommit=True)
  cur = conn.cursor()

  # Create sparkify database
  cur.execute("DROP DATABASE IF EXISTS sparkifydb")
  cur.execute("CREATE DATABASE IF NOT EXISTS sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
  cur.close()
  conn.close()

  # Connect to sparkify database
  conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
  cur = conn.cursor()

  return cur, conn


def drop_tables(cur, conn):
  """Drops all tables created on the database"""
  for query in drop_table_queries:
    cur.execute(query)
    conn.commit()

def create_tables(cur, conn):
  """Create all tables on the database"""
  for query in create_table_queries:
    cur.execute(query)
    conn.commit()

```


``` sql
-- DROP TABLES

drop_table_songplays = "DROP TABLE IF EXISTS songplays"
drop_table_users = "DROP TABLE IF EXISTS users"
drop_table_songs = "DROP TABLE IF EXISTS songs"
drop_table_artists = "DROP TABLE IF EXISTS artists"
drop_table_time = "DROP TABLE IF EXISTS time"


-- CREATE TABLES

create_table_songplays = ("""
  CREATE TABLE IF NOT EXISTS songplays 
  (songplay_id int PRIMARY KEY, 
  start_time date REFERENCES time(start_time), 
  user_id text NOT NULL REFERENCES users(user_id),
  level text, 
  song_id text REFERENCES songs(song_id),
  artist_id text REFERENCES artists(artist_id),
  session_id int,
  location text,
  user_agent text)
""")

create_table_users = ("""
  CREATE TABLE IF NOT EXISTS users
  (user_id text PRIMARY KEY,
  first_name text NOT NULL,
  last_name text NOT NULL,
  gender text,
  level text)
""")

create_table_songs = ("""
  CREATE TABLE IF NOT EXISTS songs
  (song_id text PRIMARY KEY,
  title text NOT NULL,
  artist_id text NOT NULL REFERENCES artists(Artist_id),
  year int,
  duration float NOT NULL)
""")

create_table_artists = ("""
  CREATE TABLE IF NOT EXISTS artists
  (artist_id text PRIMARY KEY,
  name text NOT NULL,
  location text,
  latitude float,
  longitude float)
""")

create_table_time = ("""
  CREATE TABLE IF NOT EXISTS time
  (start_time date PRIMARY KEY,
  hour int NOT NULL,
  day int NOT NULL,
  week int NOT NULL,
  month int NOT NULL,
  year int NOT NULL,
  weekday text NOT NULL)
""")

-- INSERT RECORDS

insert_song = ("""
  INSERT INTO songs
  (song_id, title, artist_id, year, duration)
  VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (song_id) DO NOTHING
""")

insert_artist = ("""
  INSERT INTO artists
  (artist_id, name, location, latitude, longitude)
  VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (artist_id) DO NOTHING
""")

insert_time = ("""
  INSERT INTO time
  (start_time, hour, day, week, month, year, weekday)
  VALUES (%s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (start_time) DO NOTHING
""")

insert_user = ("""
  INSERT INTO users
  (user_id, first_name, last_name, gender, level)
  VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (user_id) DO NOTHING
""")

insert_songplays = ("""
  INSERT INTO songplays
  (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (songplay_id) DO NOTHING
""")

-- SONG SELECT

song_select = ("""
  SELECT song_id, artists.artist_id, duration
  FROM songs 
  JOIN artists 
  ON songs.artist_id = artists.artist_id
  WHERE songs.title = %s
  AND artists.name = %s
  AND songs.duration = %s
""")

create_table_queries = [create_table_songplays, create_table_users, create_table_songs, create_table_artists, create_table_time]

drop_table_queries = [drop_table_songplays, drop_table_users, drop_table_songs, drop_table_artists, drop_table_time]
```

``` python
import os
import glob
import psycopg2
import pandas as pd
from sql_queries.py import *

if __name__ == "__main__":
  main()

def main():
  """ Function used to extract, transform all data from song_data and log_data and load it into a PostgreSQL DB
  """

  conn = psycopg2.connect("host: 127.0.0.1 dbname=sparkifydb user=student password=student")
  cur = conn.cursor()

  process_data(cur, conn, filepath="data/song_data", func=process_song_data)
  process_data(cur, conn, filepath="data/log_data", func=process_log_data)

  cur.close()
  conn.close()

def process_data(cur, conn, filepath, func):
  """Walks through all files nested under filepath, and processes all logs found.

    Parameters:
      cur (psycopg2.cursor()): Cursor of the sparkifydb database
      conn (psycopg2.connect()): Connection to the sparkifycdb database
      filepath (str): Filepath parent of the logs to be analyzed
      func (python function): Function to be used to process each log

    Returns:
      Name of files processed
  """

  # get all files matching extension from directory
  all_files = []
  for root, dirs, files in os.walk(filepath):
    files = glob.glob(os.path.join(root, '*.json'))
    for f in files:
      all_files.append(os.path.abspath(f))
  
  # iterate over files and process
  for i, datafile in enumerate(all_files, 1):
    func(cur, datafile)
    conn.commit()
  
  return all_files


def process_song_data (cur, datafile):
  """ Reads songs log file row by row, selects needed fields and inserts them into song and artist tables.
    
    Parameters:
      cur (psycopg2.cursor()): Cursor of the sparkifydb database
      filepath (str): Filepath of the file to be analyzed
  """

  # open song file
  df = pd.read_json(datafile, lines=True)

  for value in df.values:
    artist_id, artist_latitude, artist_longitude,artist_location, artist_name, song_id, title, duration, year = value

    # insert artist record
    artist_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude]
    cur.execute(insert_artist, artist_data)

    song_data = [song_id, title, artist_id, year, duration]
    cur.execute(insert_song, song_data)


def process_log_data (cur, datafile):
  """ Reads user activity log file row by row, filters by NextSong, selects needed fields, transforms them and inserts them into time, user and songplay tables.
  
    Parameters: 
      cur (psycopg2.cursor()): Cursor of the sparkifydb database
      filepath (str): Filepath of the file to be analyzed
  """

  # open log file
  df = pd.read_json(datafile, lines=True)

  # filter by NextSong action
  df = df[df['page'] == 'NextSong']

  # convert timestamp column to datetime
  t = pd.to_datetime(df['ts'], unit='ms')

  # insert time records
  time_data = []
  for line in t:
    time_data.append([line, line.hour, line.day, line.week, line.month, line.year, line.day_name()])
  column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
  time_df = pd.DataFrame(time_data, columns=column_labels)

  for i, row in time_df.iterrows():
    cur.execute(insert_time, list(row))

  # load user table
  user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

  # insert user records
  for i, row in user_df.iterrows():
    cur.execute(insert_user, list(row))

  # insert songplay records
  for i, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()

    if results:
      songid, artistid = results
    else:
      songid, artistid = None, None
    
    # insert songplay record
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplay_data = (i, pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
    cur.execute(insert_songplays, songplay_data)

```