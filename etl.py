import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

'''
Yield AWS IAM Credentials located in dl.cfg
'''

config = configparser.ConfigParser()
config.read('./dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['']

def create_spark_session():
    
    """
    get Spark session or initiate the Spark session on AWS Hadoop
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    """
    Purpose:
        Process song data using Spark and output as parquet to S3

    Description:
        Download JSON song/artist data from S3 bucket and set up temporary view
        so Spark SQL queries may be run on the dataset and then again loaded back
        
    Parameters:
        spark       : Spark Session
        input_data  : location of song_data JSON files with metadata about songs
        output_data : dimensional tables in parquet format will be stored
    """
    
    # read song data file
    print('Read song data from JSON file')
    df = spark.read.json(os.path.join(input_data, "song-data/A/A/A/*.json"))
    print(df.count())
    df.printSchema()
    
    # create temp view for Spark SQL queries
    print('create temp view for Spark SQL queries')
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    print('Song table: ')
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM songs_data
                            WHERE song_id IS NOT NULL
                            """).dropDuplicates(['song_id'])
    print(songs_table.limit(5).toPandas())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    print("songs.parquet completed")

    # extract columns to create artists table
    print('Artist table: ')
    artists_table = spark.sql("""
                              SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                              FROM songs_data 
                              WHERE artist_id IS NOT NULL
                              """).dropDuplicates(['artist_id'])
    print(artists_table.limit(5).toPandas())
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print("artists.parquet completed")
    
    print("process_song_data IS DONE")
    
def process_log_data(spark, input_data, output_data):
    
    """
    Purpose:
        Process log/event data using Spark and output as parquet to S3


    Description: 
        Download JSON log/event data from S3 bucket and set up temporary view
        so Spark SQL queries may be run on the dataset and then again loaded back 
        
    Parameters:
        spark       : Spark Session
        input_data  : location of song_data JSON files with metadata about events
        output_data : dimensional tables in parquet format will be stored
    """
    
    # read log data file
    print('Read log data from JSON file')
    df = spark.read.json(os.path.join(input_data,"log_data/*/*/*.json"))
    print(df.count())
    df.printSchema()
    
    # create temp view for Spark SQL queries
    print('create temp view for Spark SQL queries - logs_data')
    df.createOrReplaceTempView("logs_data")
    
    # filter by actions for song plays
    songplays_table = df['ts', 'userId', 'level','sessionId', 'location', 'userAgent'] 

    # extract columns for users table
    print('Users table: ')
    users_table = spark.sql("""
                              SELECT DISTINCT userId, firstName, lastName, gender, level
                              FROM logs_data 
                              WHERE page = 'NextSong' AND userId IS NOT NULL
                              """).dropDuplicates(['userId'])
    print(users_table.limit(5).toPandas())
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print("users.parquet completed")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # create temp view for SQL queries
    print('create temp view for Spark SQL queries - time_data')
    df.createOrReplaceTempView("time_data")
    
    # extract columns to create time table
    print('Time table: ')
    time_table = spark.sql("""
                           SELECT DISTINCT datetime as start_time,
                               hour(datetime) AS hour,
                               day(datetime) AS day,
                               weekofyear(datetime) AS week,
                               month(datetime) AS month,
                               year(datetime) AS year,
                               dayofweek(datetime) AS weekday
                           FROM time_data
                           """)
    print(time_table.limit(5).toPandas())
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
    print("time.parquet completed")

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song-data/A/A/A/*.json"))
    
    # create temp view for Spark SQL queries
    print('create temp view for Spark SQL queries - song_df')
    song_df.createOrReplaceTempView("songs_data")

    # extract columns from joined song and log datasets to create songplays table
    print('Songplays table: ')
    songplays_table = spark.sql("""
                                    SELECT monotonically_increasing_id() AS songplay_id,
                                        to_timestamp(logs_data.ts/1000) AS start_time,
                                        month(to_timestamp(logs_data.ts/1000)) AS month,
                                        year(to_timestamp(logs_data.ts/1000)) AS year,
                                        logs_data.userId AS user_id,
                                        logs_data.level AS level,
                                        songs_data.song_id AS song_id,
                                        songs_data.artist_id AS artist_id,
                                        logs_data.sessionId AS session_id,
                                        logs_data.location AS location,
                                        logs_data.userAgent AS user_agent
                                    FROM logs_data 
                                    JOIN songs_data ON logs_data.artist = songs_data.artist_name
                                """)
    print(songplays_table.limit(5).toPandas())
    
    #songplays_table = songplays_table.selectExpr("start_time")
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("songplays.parquet completed")
    
    print("process_log_data IS DONE")
    

def main():
    
    """
    Extract song/artist and log/event data from S3,
    transform it into dimensional tables format
    and load it back to S3 in Parquet format
    """
    
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3://COPY_UR_OWN_LINK_HERE"

    process_song_data(spark, input_data, output_data) 
    process_log_data(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()