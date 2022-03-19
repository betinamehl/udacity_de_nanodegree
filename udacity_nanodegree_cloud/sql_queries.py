import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_events (
                                    artist varchar,
                                    auth varchar NOT NULL,
                                    first_name varchar,
                                    gender varchar(1),
                                    item_in_session int2 NOT NULL,
                                    last_name varchar,
                                    length float4,
                                    level varchar NOT NULL,
                                    location varchar,
                                    method varchar NOT NULL,
                                    page varchar NOT NULL,
                                    registration float4,
                                    session_id int2 NOT NULL,
                                    song varchar,
                                    status int2 NOT NULL,
                                    ts int8 NOT NULL,
                                    user_agent varchar,
                                    user_id int2
                                );
                                                            
""")

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs int NOT NULL,
                                    artist_id varchar NOT NULL,
                                    artist_latitude float,
                                    artist_longitude float,
                                    artist_location varchar,
                                    artist_name varchar NOT NULL,
                                    song_id varchar NOT NULL,
                                    title varchar NOT NULL,
                                    duration float NOT NULL,
                                    year int NOT NULL
                                );
""")

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id int identity(0,1) NOT NULL,
                                start_time timestamp NOT NULL,
                                user_id int2,
                                level varchar,
                                song_id varchar,
                                artist_id varchar,
                                session_id varchar,
                                location varchar,
                                user_agent varchar,
                                primary key(songplay_id)
                            );
""")

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id int,
                            first_name varchar,
                            last_name varchar,
                            gender varchar(1),
                            level varchar NOT NULL,
                            primary key(user_id)
                        ); 
""")

song_table_create = ("""
                          CREATE TABLE IF NOT EXISTS songs (
                            song_id varchar NOT NULL,
                            title varchar NOT NULL,
                            artist_id varchar,
                            year int,
                            duration float NOT NULL,
                            primary key(song_id)
                        ); 

""")

artist_table_create = ("""
                         CREATE TABLE IF NOT EXISTS artists (
                            artist_id varchar NOT NULL,
                            name varchar NOT NULL,
                            location varchar,
                            latitude float,
                            longitude float,
                            primary key(artist_id)
                        );
""")

time_table_create = ("""
                         CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday int,
                            primary key (start_time)
                        ); 
""")

# STAGING TABLES

staging_events_copy = ("""
                        copy staging_events from 's3://udacity-dend/log_data'
                        credentials 'aws_iam_role={}'
                        compupdate off region 'us-west-2'
                        format as json 's3://udacity-dend/log_json_path.json'
                        truncatecolumns 
                        blanksasnull 
                        emptyasnull;
""").format(*config['IAM_ROLE'].values())


staging_songs_copy = ("""
                        copy staging_songs from 's3://udacity-dend/song_data'
                        credentials 'aws_iam_role={}'
                        compupdate off region 'us-west-2'
                        format as json 'auto'
                        truncatecolumns 
                        blanksasnull 
                        emptyasnull;
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id,
                                                    location, user_agent) 
                            SELECT 
                                timestamp 'epoch' + staging_events.ts / 1000 * interval '1 second' as start_time,
                                staging_events.user_id,
                                staging_events.level,
                                staging_songs.song_id,
                                staging_songs.artist_id,
                                staging_events.session_id,
                                staging_events.location,
                                staging_events.user_agent
                            FROM staging_events
                                LEFT JOIN staging_songs
                                    ON staging_events.song = staging_songs.title
                                    AND staging_events.length = staging_songs.duration
                                    AND staging_events.artist = staging_songs.artist_name 
                            WHERE page = 'NextSong'                    
""")

user_table_insert = ("""
                        INSERT INTO users (user_id, first_name, last_name, gender, level) 

                            WITH users_no_duplicates as ( 
                                SELECT
                                    user_id,
                                    first_name,
                                    last_name,
                                    gender,
                                    level,
                                    timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time,
                                    row_number() over (partition by user_id order by start_time) as row_n
                                FROM staging_events
                                WHERE user_id IS NOT NULL
                            )

                        SELECT 
                            user_id,
                            first_name,
                            last_name,
                            gender,
                            level
                        FROM users_no_duplicates
                        WHERE row_n = 1
                ;

""")

song_table_insert = ("""
                        INSERT INTO songs (song_id, title, artist_id, year, duration) 
                                SELECT 
                                    song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration

                                FROM staging_songs
                                GROUP BY 1,2,3,4,5;

""")

artist_table_insert = ("""
                        INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                            SELECT
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude

                            FROM staging_songs
                                GROUP BY 1,2,3,4,5;
""")

time_table_insert = ("""
                        INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                            SELECT 
                                    timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time,
                                    extract(hour from start_time) as hour,
                                    extract(day from start_time) as day,
                                    extract(week from start_time) as week,
                                    extract(month from start_time) as month,
                                    extract(year from start_time) as year,
                                    extract(weekday from start_time) as weekday
                               
                            FROM staging_events
                            GROUP BY 1,2,3,4,5,6,7
                                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
