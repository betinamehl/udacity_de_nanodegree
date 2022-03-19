import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Process song files and insert data into the database
    
    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param filepath: the filepath of the file that contains the data and will be processed 
    :type filepath: string
    
    """
    # open song file
    df = pd.read_json(filepath, orient='columns', lines=True)

    # insert song record
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_columns].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artist_columns].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Process log files and insert data into the database
    
    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param filepath: the filepath of the file that contains the data and will be processed 
    :type filepath: string
    
    """
    # open log file
    df = pd.read_json(filepath, orient='columns', lines = True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('starttime', 'hour', 'day', 'week', 'month', 'year', 'weekday' )
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_columns]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, 
                         artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Get all data files and process them to store in the database
    
    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param conn: database connection
    :type conn: postgres (psycopg2) database connection object
    :param filepath: the filepath of the file that contains the data and will be processed 
    :type filepath: string
    :param func: function that process files and inserts them into the database
    :type func: python function
    
    """
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