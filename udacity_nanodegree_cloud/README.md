# Sparkfy ETL database project


## Purpose

In the past couple of months we've experiencing a real exciting growth in our user base. Due to this increase we've bee dealing with some problems in our postgres database used for storing our analytical data.

Knowing that, we've decided to move our processes and data onto the cloud. 

We will now use s3 to store the raw data and Redshift to store the processed data. 


## Running the .py scripts

This project contains three .py files that need to be executed in order to process the files into the database:

- create_cluster.py (only if the cluster is not already online)
- create_tables.py
- etl.py

They need to be run in the order described above. 

To run the python files, open the terminal in the project folder and type 'python + the file name' (or type the complete file path in any folder).

Obs.: in order to run the files you will need the python package installed in your computer. You can download it [here](https://www.python.org/downloads/).

## Files within this project

In addition to this readme file and the data files (inside the data folder), this project includes five files:

- etl.py: file that contains the python code that process the data and inserts it into the database
- create_cluster.py: file that contains the python code that create the redshift cluster 
- create_tables.py: file that contains the python code that drops (if they exist) and creates the tables in the database
- sql_queries.py: file that contains the queries used in the etl process. It is used within the etl.py and create_tables.py.
- cdwh.cfg: config data for the ETL process


## Schema design

We've chosen to design our database using the star schema concept. This will help our business analysts to query the data in a simpler way. 

The ETL process creates 5 tables, being 1 fact table and 4 dimension tables:

#### Fact Tables:

1. Songplays: records in log data associated with song plays i.e. records with page NextSong

    | Column      | Type        | Description              |
    | ----------- | ----------- | ------------------------ |
    | songplay_id | integer | songplay identification code (PK) |
    | start_time | timestamp | songplay time |
    | user_id | integer | user identification code (FK) |
    | level | varchar | session subsciption category (free or paid) |
    | song_id | varchar | song identification code (FK) |
    | artist_id | varchar | artists identification code (FK) |
    | session_id | varchar | session identification code |
    | location | varchar | songplay location |
    | user_agent | varchar | songplay agent (computer/mobile/browser/etc) |

#### Dimension Tables

1. Users: information about users in the app
    
    | Column      | Type        | Description              |
    | ----------- | ----------- | ------------------------ |
    | user_id | integer | user identification code (PK) |
    | fist_name | varchar | user first name |
    | last_name | varchar | user last name |
    | gender | varchar | user gender, F or M |
    | level | varchar | user subsciption type: free or paid |

2. Songs: information about songs that are stored in the app

    | Column      | Type        | Description              |
    | ----------- | ----------- | ------------------------ |
    | song_id | varchar | song identification code (PK) |
    | title | varchar | song title | 
    | artist_id | varchar | artist indentification code (FK) |
    | year | integer | song release year |
    | duration | float | song duration in seconds | 

3.  Artists: information about the artists stored in the app

    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    |  artist_id | varchar | artist identification code (PK) |
    |  name | varchar | artist name |
    |  location | varchar | artitst location |
    |  latitude | float | latitude coordinate of the artist location | 
    |  longitude | float |  longitude coordinate of the artist location
    
4. Time: table containing time attributes

    |  Column | Type | Description |
    | --------- | ------ | ------------- | 
    |  star_time | timestamp | complete timestamp |
    |  hour | integer | hour attribute |
    |  day | integer | day attribute |
    |  month | integer | month attribute (1-12) | 
    |  year | integer | year attribute
    |  week | integer | week attribute | 
    |  weekday | integer | weekday attribute (0 = sunday) |
  