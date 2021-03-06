# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id SERIAL PRIMARY KEY, 
        start_time time NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender varchar, 
        level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar, 
        year int, 
        duration int);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY, 
        name varchar NOT NULL, 
        location varchar,
        lattitude varchar, 
        longitude varchar);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time time PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")


#in the future we might not want to exclude the level if we can tie the paid/not paid customer data as it #updates from another table. So we can potentially add another column that is like pervious_paid y/n and learn 
# if they were once a paying user.

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name, 
    last_name,
    gender, 
    level)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id)
DO UPDATE SET 
    first_name = excluded.first_name,
    last_name = excluded.last_name,
    gender = excluded.gender,
    level = excluded.level
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration)
    VALUES (%s,%s,%s,%s,%s)
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location,
    lattitude, 
    longitude)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) 
DO NOTHING

""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week, 
    month, 
    year, 
    weekday)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time) 
DO NOTHING


""")

# FIND SONGS

song_select = ("""
    SELECT song_id, artists.artist_id 
    FROM songs 
    JOIN artists on songs.artist_id = artists.artist_id 
    WHERE title = %s and name = %s and duration = %s
""")

# QUERY LISTS

create_table_queries = [
        songplay_table_create, 
        user_table_create, 
        song_table_create, 
        artist_table_create, 
        time_table_create
]
drop_table_queries = [
        songplay_table_drop, 
        user_table_drop, 
        song_table_drop, 
        artist_table_drop, 
        time_table_drop
]