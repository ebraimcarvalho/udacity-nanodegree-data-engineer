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

drop_table_queries = [drop_table_songplays, drop_table_users, drop_table_songs, drop_table_artists, drop_table_time],
```