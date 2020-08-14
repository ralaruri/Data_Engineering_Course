import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#Each Staging table is a like a full view of the all the available data which we can model smaller higher performance tables
#What we model off  
#Events table is based ont he log events data in S3
#song table is based on the song_plays in S#

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length DOUBLE PRECISION,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration BIGINT,
        sessionId integer,
        song varchar,
        status integer,
        ts BIGINT,
        userAgent varchar,
        userId integer
    );

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs integer,
        artist_id varchar,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year integer
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY sortkey,
        start_time timestamp,
        user_id integer NOT NULL,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        level varchar,
        session_id integer,
        location varchar,
        user_agent varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer PRIMARY KEY distkey,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level  varchar NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY NOT NULL,
        artist_id varchar distkey NOT NULL,
        title varchar,
        year integer,
        duration float
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY NOT NULL SORTKEY,
        artist_name varchar NOT NULL,
        location varchar,
        latitude varchar,
        longitude varchar
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
        hour integer NOT NULL,
        day integer NOT NULL,
        week integer NOT NULL,
        month integer NOT NULL,
        year  integer NOT NULL,
        weekday integer NOT NULL
    );
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} 
    credentials 'aws_iam_role={}' 
    format as json {}
    STATUPDATE ON region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
  COPY staging_songs FROM {} 
      credentials 'aws_iam_role={}' 
      format as json 'auto'
      STATUPDATE ON region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (start_time, user_id, song_id, artist_id, level,
                            session_id, location, user_agent)
SELECT 
       DISTINCT TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' as start_time,
       events.userId as user_id, 
       events.level as level, 
       songs.song_id as song_id,
       songs.artist_id as artist_id,
       events.sessionId as session_id,
       events.location as location, 
       events.userAgent as user_agent
FROM staging_events as events
JOIN staging_songs as songs ON (events.artist = songs.artist_name) 
WHERE events.page = 'NextSong';

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, 
                       level)
    
    SELECT
        DISTINCT events.userId as user_id,
        events.firstName as first_name,
        events.lastName as last_name,
        events.gender as gender,
        events.level as level
    FROM staging_events as events
    WHERE events.page = 'NextSong';
    
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, artist_id, title, year, 
                       duration)
    
    SELECT
        DISTINCT songs.song_id as song_id,
        songs.artist_id as artist_id,
        songs.title as title, 
        songs.year as year,
        songs.duration as duration
    FROM staging_songs as songs;
    
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, artist_name, location, latitude, 
                      longitude)
                      
    SELECT 
        DISTINCT songs.artist_id as artist_id,
        songs.artist_name as artist_name,
        songs.artist_location as location,
        songs.artist_latitude as latitude,
        songs.artist_longitude as longitude
    FROM staging_songs as songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month,
                     year, weekday)
    
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
        EXTRACT(hour FROM start_time) as hour,
        EXTRACT(day FROM start_time) as day,
        EXTRACT(week FROM start_time) as week,
        EXTRACT(month FROM start_time) as month,
        EXTRACT(year FROM start_time) as year,
        EXTRACT(weekday FROM start_time) as weekday
    FROM staging_events
    WHERE page = 'NextSong'
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
