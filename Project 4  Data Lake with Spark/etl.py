import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StringType
from pyspark.sql.functions import from_unixtime
import pandas

#this is temporary when I was testing dataframes in pandas and parquet 
#sc.install_pypi_package("pandas==0.25.1")


#need to change the config to include KEYS in order to properly read it.
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    
    This function is to create the intital connection Spark Session
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    
    This function's purpose is to pull the song data from S3 (Udacity S3 Bucket) and filter and organize the data into 
    song and artist table and then back into my personal S3 bucket.

    """
    # get filepath to song data file
    # input_data = "s3a://udacity-dend/" completes the full s3 bucket url
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df_song = spark.read.json(song_data)
    
    df_song.createOrReplaceTempView("temp_song")

    # extract columns to create songs table
    songs_table = spark.sql (""" 
        select
            songs.title,
            songs.aritst_id,
            songs.year,
            songs.duration,
        from temp_song as songs """)
    
    songs_table = songs_table.dropDuplicates(['title', 'artist_id'])

    songs_table.write.parquet(output_data + 'songs_table', partitionBy=['year', 'artist_id'], mode = 'Overwrite')


    #write songs table to parquet files partitioned by year and artist

    #extract columns to create artists table
    artists_table = spark.sql (""" 
        select
            artists.artist_id,
            artists.artist_name,
            artists.artist_location,
            artists.artist_latitude,
            artists.artist_longitude
        from temp_song as artists """)

    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files
    # artists_table
    artists_table.write.parquet(output_data + 'artist_table', mode='Overwrite')

def process_log_data(spark, input_data, output_data):
    """
    
    This function is similar to the  the song data one but its purpose is to create the log, users, songplays table from S3 (Udacity S3 Bucket) 
    and filter and organize the data into song and artist table and then back into my personal S3 bucket.
    The time table requires some data modeling to extract into datetime and hour, week, days etc. which is a bit different than the other tables.

    """
    
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    
    df = df_log.filter(F.col("page") == "NextSong")
    
    # extract columns for users table   
    
    df.createOrReplaceTempView("temp_log")
    
    users_table = spark.sql("""
        select 
            users.userId as user_id,
            users.firstName as first_name,
            users.lastName as last_name,
            users.gender as gender,
            users.level as level
        from temp_log users """)
    
    users_table = users_table.dropDuplicates(['user_id'])
    
    users_table.write.parquet(output_data + 'users_table', mode='Overwrite')
    
    # create timestamp column from original timestamp column
    ## get_timestamp = udf(lambda x: str(int(x) // 1000))
    ## df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    ## get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    ## df = 
    
    # extract columns to create time table
    time_table = spark.sql ("""
        select
            time_table.start_time as start_time
            hour(time_table.start_time) as hour,
            dayofmonth(time_table.start_time) as day,
            weekofyear(time_table.start_time) as week,
            month(time_table.start_time) as month,
            dayofweek(time_table.start_time) as weekday
        from 
            (select to_timestamp(time_table_temp.ts/1000) as start_time
            from temp_log as time_table_temp))as time_table """)
    

    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table', partitionBy=['year', 'month'] mode='Overwrite')


    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" 
        select
            monotonically_increasing_id() as songplay_id,
            to_timestamp(log_data.ts/1000) as start_time,
            month(log_data.ts/1000) as month,
            year(log_data.ts/1000) as year,
            log_data.userId as user_id,
            log_data.level as level,
            song_table.song_id as song_id,
            song_table.artist_id as artist_id,
            log_data.sessionId as session_id,
            log_data.location as location,
            log_data.userAgent as user_agent
        from 
            temp_log as log_data
            join song_temp song_table on
            log_data.artist = song_table.artist AND
            log_data.song = song_table.title""")
    
    songplays_table = songplays_table.dropDuplicates(['songplay_id'])



    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_table' partitionBy=['year', 'month'], mode = 'Overwrite')


def main():
    """
    The main function's purpose is to set up the input and output data (the personal and udacity s3 buckets).
    Along with running prior functions a full program to extract and run throught the the extraction and modeling of the data
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ramzi-dend-bucket/"
    
    process_song_data(spark, input_data, output_data) 
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()