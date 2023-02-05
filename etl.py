import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.1,")\
        .config('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .config('fs.s3a.access.key', AWS_ACCESS_KEY_ID)\
        .config('fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY)\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def process_song_data(spark, output_data):
    # get filepath to song data file
    song_data = 'song_data'
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, '/songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                    .withColumnRenamed('artist_name', 'name') \
                    .withColumnRenamed('artist_location', 'location') \
                    .withColumnRenamed('artist_latitude', 'lattitude') \
                    .withColumnRenamed('artist_longitude', 'longitude') \
                    .dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, '/artists/artists.parquet'), 'overwrite')


def process_log_data(spark, output_data):
    # get filepath to log data file
    log_data = 'log_data'

    # read log data file
    events_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    events_df = events_df.filter(events_df.page == 'NextSong')

    # extract columns for users table    
    users_table = events_df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
                .withColumnRenamed('firstName', 'first_name') \
                .withColumnRenamed('lastName', 'last_name') \
                .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, '/users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    events_df = events_df.withColumn('timestamp', get_timestamp(events_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    events_df = events_df.withColumn('datetime', get_datetime(events_df.ts))
    
    # extract columns to create time table
    time_table = events_df.select('datetime') \
                .withColumn('start_time', events_df.datetime) \
                .withColumn('hour', hour('datetime')) \
                .withColumn('day', dayofmonth('datetime')) \
                .withColumn('week', weekofyear('datetime')) \
                .withColumn('month', month('datetime')) \
                .withColumn('year', year('datetime')) \
                .withColumn('weekday', dayofweek('datetime'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, '/time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    # ***I am using song_df from first read song_data on top of this program
    song_df.printSchema()

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = events_df.join(song_df, events_df.artist == song_df.artist_name, 'inner')
    songplays_table = joined_df.select(
        col('datetime').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    ).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, '/songplays/songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    output_data = "s3a://<your bucket>/"
    
    process_song_data(spark, output_data)    
    process_log_data(spark, output_data)


if __name__ == "__main__":
    main()
