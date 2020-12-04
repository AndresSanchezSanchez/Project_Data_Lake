import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import IntegerType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.11.538,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''
    Developing a Spark SQL application.
    
    As a Spark developer, you create a SparkSession using the SparkSession.builder method (that gives you access to Builder API that you use to configure the session).

    1. builder: Object method to create a Builder to get the current SparkSession instance or create a new one.
    2. config: Sets a config option. Options set using this method are automatically propagated to both SparkConf and SparkSession's own configuration.
    3. spark.jars.packages: Comma-separated list of Maven coordinates of jars to include on the driver and executor classpaths. The coordinates should be groupId:artifactId:version. If spark.jars.ivySettings is given artifacts will be resolved according to the configuration in the file, otherwise artifacts will be searched for in the local maven repo, then maven central and finally any additional remote repositories given by the command-line option --repositories. For more details, see Advanced Dependency Management.
    4. getOrCreate: getOrCreate(conf=None) Get or instantiate a SparkContext and register it as a singleton object.
    
    Must return a new Spark session to work with AWS
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.2") \
        .getOrCreate()
#    spark = SparkSession \
#        .builder \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
#        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    Work with the songs data with Spark and store the output.
        
    KEYS WORDS:
    spark - OBJECT
    input_data - FILE PATH TO INPUT DATA FILES
    output_data: FILE PATH TO STORE OUTPUT DATA FILES
    """
    # get filepath to song data file
    ###To avoid the delays and the program takes long time to run, the file of songs looks just for the file which starts song_data/A/B/C/*.json###
    song_data = input_data + 'song_data/A/B/C/*.json'

    #song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    print(song_data)

    df = spark.read.json(song_data)
    df.printSchema()
    df.show(10)
    
    print(df)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id','year','duration').distinct()
    print(songs_table)
    songs_table.show(10)

    print(output_data)
    s=output_data+'songs_table/'
    print(s)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path = output_data + 'songs_table/', partitionBy = ('year', 'artist_id'))
    #songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    #songs_table .write .partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')
    #songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet')
    

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    print(artists_table)
    artists_table.show(10)

    print(output_data)
    s=output_data+'artists_table/'
    print(s)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + 'artists_table/')
    #artists_table.write.mode('overwrite').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """ 
    Work with the songs data with Spark and store the output.
        
    KEYS WORDS:
    spark - OBJECT
    input_data - FILE PATH TO INPUT DATA FILES
    output_data: FILE PATH TO STORE OUTPUT DATA FILES
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.select('*').where(df['page'] == 'NextSong')
    
    # extract columns for users table
    users_table = df.select(df['userId'].alias('user_id'), \
                            df['firstName'].alias('first_name'), \
                            df['lastName'].alias('last_name'), \
                            df['gender'], \
                            df['level']).distinct()
    
    print(users_table)
    users_table.show(10)

    print(output_data)
    s=output_data+'users_table/'
    print(s)
        
    # write users table to parquet files
    users_table.write.parquet(path = output_data + 'users_table/')
    #users_table.write.mode('overwrite').parquet(output_data + 'users/')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df = df.withColumn('datetime', from_unixtime('start_time'))

    
    # extract columns to create time table
    time_table = df.select('start_time', \
                           hour('datetime').alias('hour'), \
                           dayofmonth('datetime').alias('day'), \
                           weekofyear('datetime').alias('week'), \
                           month('datetime').alias('month'), \
                           year('datetime').alias('year'), \
                           date_format('datetime', 'u').alias('weekday'))
    
    print(time_table)
    time_table.show(10)

    print(output_data)
    s=output_data+'time_table/'
    print(s)
   
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(path = output_data + 'time_table/', partitionBy = ('year', 'month'))
    #time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/B/C/*.json')
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df['song'] == song_df['title']) \
    .select(monotonically_increasing_id().alias('songplay_id'), \
            'start_time', \
            year('datetime').alias('year'), \
            month('datetime').alias('month'), \
            df['userId'].alias('user_id'), \
            'level', \
            'song_id', \
            'artist_id', \
            df['sessionId'].alias('session_id'), \
            'location', \
            df['userAgent'].alias('user_agent'))

    print(songplays_table)
    songplays_table.show(10)
    
    print(output_data)
    s=output_data+'songplays_table/'
    print(s)

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(path = output_data + 'songplays_table', partitionBy = ('year', 'month'))
    #songplays_table.write.moder('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays/')

def main():
    
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3://aws-logs-134002223423-us-west-2/elasticmapreduce/j-1M50RNUVDI7Q/"
    #output_data = "s3a://20200816as2106/"
    #output_data = "s3a://aws-logs-134002223423-us-west-2/"
    
    input_data = config.get('IO', 'INPUT_DATA')
    output_data = config.get('IO', 'OUTPUT_DATA')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()