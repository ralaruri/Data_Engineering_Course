# Data Engineer Project 1/5 Data Modeling with Postgres

0. Prereqs to run this program 
* Python 3.+ is required along with psycopg2, pandas libaries installed.
* Utilzing psycopg2 might include the installation of Postgres if nto already installed on your machine.
* order of running this

1. Purpose of Database
* This database is built out using a star schema with a central fact table "songplays" and 4 dimensional tables users, songs, artists, time
* The purpose of this is to build an analytic schema in order to be able to query the individual tables along with more complex joined queries
* The complex join queries allow you to understand user behavior along with details of the songs and artist they listen to


2. Database schema and ETL pipeline
* The data that is included in the database comes from two sets of datasets. Songs which are the details of the songs and artists, and log files which are the actual users utilizing the steaming service.
* The Songplays as mentioned before is central to this start schemea it includes a user_id , song_id, artist_id, startime fields that are able to join directly to the users table, songs table, artists table, and time table respectively. 
* The data files are in the json data type that requires some tranformation using Pandas.
* Steps to create database
    1. Create the tables and the insert queries for each table in the sql_queries.py (along with the drop table statements to troublehsoot and run) 
    2. Building the ETL process for each table in the etl.py file; most json files were easy to read and process, some cause diffcuility such as the time table which required some more extensive transformation especially if not fimialr with timestamps and utilziation of them. 


3. Example Queries
    1. SELECT count(distinct user_id), level FROM users group by level;
    * with this query you are able to figure out the amount of paid vs. free users quickly querying a single table that isn't clutter with non-nesscary fields. with high read performance 
    
    2. 
        with songplays as (select * from songplays), 

        users as (select * from users),

        songs as (select * from songs),

        joined_users as (
                select songplays.*, users.first_name, users.last_name, users.gender from songplays 
                left join users 
                ON songplays.user_id = users.user_id
        )


        select 
            distinct(user_id),
            first_name, 
            last_name,
            gender,
            level,
            count(songplay_id) as songplay_count 
        from joined_users  
        group by user_id, first_name, last_name, gender, level
        order by songplay_count desc;
        
      * The above query shows the power in a star schema when i want more complex queries, I can do a simple join and learn more about the users, for
        example I can see the top 5 users are paid users and are all female. 
      * I can further filter and build queries to understand how long these customers with a further additon of a paying_transaction table, and look
        at the least active paid users and derive some insights if they are likely to churn or how we can potentially engaged them to be longer    
        susitaining customers. 


4. Issues 
    1. Timestamp issue in the time table, timestamptz, or timestamp data types do not function with the
        dataframe the inset statement.
        * solution: you need to use type time. 
        * issue i had with this is i've learn its poor data handling to use not use a timestamp function
            whenever dealing with time. 
    2. Duplicate Primary Key in Users table
        * Solution: Using a ON CONFLICT PK (user_id) statement when creating the table to update the primary key
            value
    3. Duplicate PK on artist_id when running the etl.py
        * psycopg2.IntegrityError: duplicate key value violates unique constraint "artists_pkey"
            DETAIL:  Key (artist_id)=(ARNTLGG11E2835DDB9) already exists.
        * simple soultion was to create on conflict do nothing; not sure if actually the duplicates woudl hurt the data
    4. Duplicate PK on start_time when running the etl.py
        * cur.execute(time_table_insert, list(row))
            psycopg2.IntegrityError: duplicate key value violates unique constraint "time_pkey"
            DETAIL:  Key (start_time)=(17:18:01.796) already exists.
        * defaulted to the above quick fix (issue 3) due to having duplicate timestamps is feasible

* After these fixes it complied etl.py and I was able to query the database!!

5. Resources Used
 * Udacity Knowledge Q/A and Student Hub 
 * https://dba.stackexchange.com/questions/49768/duplicate-key-violates-unique-constraint
 * https://www.google.com/search?client=safari&rls=en&ei=pTguXpuKE8XJ5gLr84egCw&q=postgresql+datatypes&oq=postgresql+datatypes&gs_l=psy-ab.3..0i10l10.1998.3030..3145...0.2..0.79.732.10......0....1..gws-wiz.......0i71j0i67j0.ZedvhzcSv-0&ved=0ahUKEwjb8OG-zKLnAhXFpFkKHev5AbQQ4dUDCAo&uact=5
 * https://dba.stackexchange.com/questions/134493/upsert-with-on-conflict-using-values-from-source-table-in-the-update-part
 
 *https://www.epochconverter.com
 *https://wiki.postgresql.org/wiki/Don%27t_Do_This