import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')




def create_spark_session():
    """
    Get exist spark session or create new if not exist
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Proccess all raw song data with format JSON from input folder (S3) and save into output folder as analytics table (S3)
    We will create 2 table:
    1. dim_songs: dimention table which save songs information
    2. dim_artists: dimention table which save artists information
    Function param:
    :param spark: Spark session
    :param input_data: input data folder path
    :param output_data: output data folder path
    """
    # get filepath to song data file
    
    # get data file from local
#     song_data = "data/song-data/song_data/A/A/A/*.json"
    # get data file from S3 bucket
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    dim_songs = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    dim_songs.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'dim_songs')


    # extract columns to create artists table
    dim_artists = df.select(col('artist_id'), 
                              col('artist_name').alias('name'), 
                              col('artist_location').alias('location'), 
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    dim_artists.write.mode('overwrite').parquet(output_data + 'dim_artists')


def process_log_data(spark, input_data, output_data):
    """
    Proccess all raw log data with format JSON from input folder (S3) and save into output folder as analytics table (S3)
    We will create 3 table:
    1. fact_songplays: fact table which save records in log data link with songs play with page is Next Song
    2. dim_time: time table with timestamp recorded
    3. dim_users: users table with data of users
    Function param:
    :param spark: Spark session
    :param input_data: input data folder path
    :param output_data: output data folder path
    """
    # get filepath to log data file
    
    # get data file from local
#     log_data ="data/log-data/*.json"
    # get data file from S3 bucket
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    dim_users = df.select(col('userId').alias('user_id'), 
                          col('firstName').alias('first_name'), 
                          col('lastName').alias('last_name'),
                          col('gender'), 
                          col('level')).dropDuplicates()
    
    # write users table to parquet files
    dim_users.write.mode('overwrite').parquet(output_data + 'dim_users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('datetime', get_datetime(df.timestamp))
        
    # extract columns to create time table
    df =  df.withColumn('start_time', df.datetime)\
                .withColumn('hour', hour('datetime'))\
                .withColumn('day', dayofmonth('datetime'))\
                .withColumn('week', weekofyear('datetime'))\
                .withColumn('month', month('datetime'))\
                .withColumn('year', year('datetime'))\
                .withColumn('weekday', dayofweek('datetime')).dropDuplicates()
    
    dim_time = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    dim_time.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'dim_time')

    # read in song data to use for songplays table
    # get data file from local
#     song_df = spark.read.json('data/song-data/song_data/A/A/A/*.json')
    # get data file from S3 bucket
    song_df = spark.read.json(input_data + 'song-data/*/*/*/*.json')
    song_df = song_df.alias('songs_df')
    
    # extract columns from joined song and log datasets to create songplays table 
    df = df.alias('logs_df')
    df_log_join_song = df.join(song_df, col('logs_df.artist') == col('songs_df.artist_name'), 'inner')
    fact_songplays = df_log_join_song.withColumn('songplay_id', monotonically_increasing_id())\
                                        .select(col('logs_df.datetime').alias('start_time'),
                                                col('logs_df.userId').alias('user_id'),
                                                col('logs_df.level').alias('level'),
                                                col('songs_df.song_id').alias('song_id'),
                                                col('songs_df.artist_id').alias('artist_id'),
                                                col('logs_df.sessionId').alias('session_id'),
                                                col('logs_df.location').alias('location'),
                                                col('logs_df.userAgent').alias('user_agent'),
                                                year('logs_df.datetime').alias('year'),
                                                month('logs_df.datetime').alias('month'))
    
    # write songplays table to parquet files partitioned by year and month
    fact_songplays.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'fact_songplays')


def main():
    """
    Main function
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://</Insert your S3 bucket path>/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
