Project 4: Data Lake with Amazon EMR
====================================

Udacity Data Engineer Nanodegree project
----------------------------------------

### Issue to solve in the project

An imagined music streaming startup named Sparkify has grown their user base and song database and now want to take their data to the data lake. Their data are in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim of this project is the process that extracts data from S3, processes them using Spark and Amazon EMR, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Schema design

The star schema has 1 fact table (songplays), and 4 dimension tables (artists, songs,  time, users).

![](relationalschema.png)

The project is based on two Amazon Web Services: S3 and EMR.

Data sources are provided by two public S3 buckets:

`Song data: s3://udacity-dend/song_data`

`Log data: s3://udacity-dend/log_data`

Song data bucket contains info about songs and artists. Log data bucket has info concerning actions done 
by users (f.ex. which song they are listening). The objects contained in both buckets are JSON files. 

### ETL pipeline

1. Set up a config file `dl.cfg`. Put in the information for your cluster and IAM-Role that can manage your cluster and read S3 buckets.
2. Specify output data path in the main function of `etl.py`
3. Run `etl.py` to read the database credentials from the config file, connect to the database, load from S3 JSON files and create the final data lake by back load of these dimensional process to S3.
