Project: Data Lake
Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths one files in this dataset.

- song_data/A/B/C/TRABCEI128F424C983.json

The metho to localizate every files will be 'song_data/*/*/*/*.jason' (we just used the data 'song_data/A/B/C/*.json' to avoid when we run the program, this takes a long time).

The second dataset consists of log files in JSON which will be working with are partitioned by year and month.

- log_data/2018/11/2018-11-12-events.json

The schema for song play analysis will get five parts (the fact table and four dimension tables) which are shown below.

FACT TABLE

1. songplays - ecords in log data associated with song plays i.e. records with page NextSong.
    * songplay_id, start_time, user_id, level, song_id, artist_id, location, user_agent.
    
DIMENSION TABLES

2. users - users in the app.
    * user_id, first_name, last_name, gender, level.

3. songs - songs in music database.
    * song_id, title, artist_id, year, duration.

4. artist - artists in music database.
    * artist_id, name, location, lattitude, longitude.
    
5. time - timestamos of records in songplays broken down into specific units.
    * start_time, hour, day, week, month, year, weekday.
    
First of all, there are to process every data which they are inside 'song_data/A/B/C/*.json' and those data are in the tables 'songs_table' and 'artists_table' which we used and kept in a bucket which I had created previously

When data have been kept in my bucket, they will be processed to organize in their fact and dimensions that they are named before and were explained which they must be their parts.