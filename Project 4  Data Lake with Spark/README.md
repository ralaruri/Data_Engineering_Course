# PySpark on AWS EMR

The purpose of this project is to set up a ETL pipeline from S3 processing the data using Spark (SQL and python) and then loading the data that is processed into a different S3 Bucket .
## Getting Started

You'll need to create an EMR cluster on AWS along with a ssh key-pair in order to SSH into it on your local computer. 

If you haven't already set up a personal S3 bucket or you can write to the S3 bucket when creating your EMR service (please note collapasing and deleting your cluster you can potetially delete the assoicated S3 bucket)

### Prerequisites

* AWS Account
* Python/SQL knowledge
* Spark Knowledge


### Installing and setting up
* Tesing your spark code in a EMR notebook is useful and the path I took.

### Setting up Config File (dl.cfg)

* Make sure there is a root name in the file such as [AWS] or [Keys] otherwise you will have trouble reading your AWS credentials.
```
os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
```
```
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']
```

### Reading out of S3 Bucket

* your song path and log path is an extension of the s3 bucket with the data and then a series of wildcard * to be able to read the whole buckets files with the same structure example:

```
input_data = "s3a://udacity-dend/" 
```
```
song_data = input_data + 'song_data/*/*/*/*.json'
```


### Process Song, Artist, Users, Time and Songplay Tables

* For each table (Besides the Time and Songplay Table which are based on the log and song data combined or transformed) you need read the raw data which is in a json format using a spark.read.json function. 

* from that point you might want to create a temporary table in order to keep the raw data the same if you need to re-access it later. 

* afterwards you can query that data as using plain SQL but please note spark.sql has its nuances and its important to learn some new functions to do what you do in a traditional RDBMS

#### read song data file example
```
    df_song = spark.read.json(song_data)
    
    df_song.createOrReplaceTempView("temp_song")

    songs_table = spark.sql (""" 
        select
            songs.title,
            songs.aritst_id,
            songs.year,
            songs.duration,
        from temp_song as songs """)

```

### Writing to a Parquet

* You want to write each table your transformed as a parquet file into your s3 bucket 
* an example below

```
songs_table.write.parquet(output_data + 'songs_table', partitionBy=['year', 'artist_id'], mode = 'Overwrite')
```


### Writing to an S3 Bucket

* You want to write each table your transformed as a parquet file into your s3 bucket 
* Set your output data as your S3 buckt in your final main function.

```
    output_data = "s3a://ramzi-dend-bucket/"

```





## Built With

* [AWS-EMR](https://aws.amazon.com/emr/) - AWS EMR documentation
* [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) - PySpark documentation


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments and Resources Used

* Hat tip to anyone whose code was used and helped guide me to complete this project
* [EMR](https://towardsdatascience.com/getting-started-with-pyspark-on-amazon-emr-c85154b6b921) - Getting Started with EMR a great guide how to trouble shoot and setup your EMR service
* [Udacity](https://knowledge.udacity.com/questions/111529) - Udfs for timestamps
* [Stackoverflow](https://knowledge.udacity.com/questions/111529) - PySpark not installed on your EMR cluster?
* [Udacity](https://knowledge.udacity.com/questions/46508) - How to read data from s3
* [AWS](https://aws.amazon.com/blogs/big-data/install-python-libraries-on-a-running-cluster-with-emr-notebooks/) - How to install more python libraires on your spark cluster
* [AWS](https://aws.amazon.com/premiumsupport/knowledge-center/emr-pyspark-python-3x/)-support if your having trouble getting your cluster running
