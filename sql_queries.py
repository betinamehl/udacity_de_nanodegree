# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""

    CREATE TABLE songplays (
        songplay_id serial PRIMARY KEY NOT NULL,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id varchar,
        location varchar,
        user_agent varchar);                
        
""")

user_table_create = ("""

    CREATE TABLE users (
        user_id int PRIMARY KEY NOT NULL,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar); 
""")

song_table_create = ("""

     CREATE TABLE songs (
        song_id varchar PRIMARY KEY NOT NULL,
        title varchar NOT NULL,
        artist_id varchar,
        year int,
        duration float NOT NULL); 
""")

artist_table_create = ("""
    
    CREATE TABLE artists (
        artist_id varchar PRIMARY KEY NOT NULL,
        name varchar NOT NULL,
        location varchar,
        latitude float,
        longitude float); 
""")

time_table_create = ("""

    CREATE TABLE time (
        star_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int); 
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id,
                                                    location, user_agent) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT (songplay_id) 
                           DO UPDATE
                           SET start_time = COALESCE(excluded.start_time,  songplays.start_time),
                               user_id = COALESCE(excluded.user_id,  songplays.user_id),
                               level = COALESCE(excluded.level,  songplays.level),
                               song_id = COALESCE(excluded.song_id,  songplays.song_id),
                               artist_id = COALESCE(excluded.artist_id,  songplays.artist_id),
                               session_id = COALESCE(excluded.session_id,  songplays.session_id),
                               location = COALESCE(excluded.location,  songplays.location),
                               user_agent = COALESCE(excluded.user_agent,  songplays.user_agent);
                           
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) 
                                VALUES (%s, %s, %s, %s, %s)
                         ON CONFLICT (user_id) 
                           DO UPDATE
                           SET first_name = COALESCE(excluded.first_name,  users.first_name),
                               last_name = COALESCE(excluded.last_name,  users.last_name),
                               gender = COALESCE(excluded.gender,  users.gender),
                               level = COALESCE(excluded.level,  users.level);
                         
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) 
                                VALUES (%s, %s, %s, %s, %s)
                         ON CONFLICT (song_id) 
                           DO UPDATE
                           SET title = COALESCE(excluded.title,  songs.title),
                               artist_id = COALESCE(excluded.artist_id,  songs.artist_id),
                               year = COALESCE(excluded.year,  songs.year),
                               duration = COALESCE(excluded.duration,  songs.duration);
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                                VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (artist_id) 
                           DO UPDATE
                           SET name = COALESCE(excluded.name,  artists.name),
                               location = COALESCE(excluded.location,  artists.location),
                               latitude = COALESCE(excluded.latitude,  artists.latitude),
                               longitude = COALESCE(excluded.longitude,  artists.longitude);
""")


time_table_insert = (""" INSERT INTO time (star_time, hour, day, week, month, year, weekday) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                          ON CONFLICT (star_time) 
                          DO UPDATE
                           SET hour = COALESCE(excluded.hour,  time.hour),
                               day = COALESCE(excluded.day,  time.day),
                               week = COALESCE(excluded.week,  time.week),
                               month = COALESCE(excluded.month,  time.month),
                               year = COALESCE(excluded.year,  time.year),
                               weekday = COALESCE(excluded.weekday,  time.weekday);
""")

# FIND SONGS

song_select = (""" SELECT 
                       songs.song_id,
                       songs.artist_id
                   FROM songs
                       LEFT JOIN artists on artists.artist_id = songs.artist_id
                   WHERE songs.title = %s
                       AND artists.name = %s
                       AND songs.duration = %s

""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]