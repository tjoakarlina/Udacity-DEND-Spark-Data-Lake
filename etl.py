import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, monotonically_increasing_id, weekofyear, dayofweek

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

config = configparser.ConfigParser()
config.read('dl.cfg')

# If we are running spark locally we need the AWS Access Key ID and AWS Secret Access Key to access S3
# If we are running spark in the AWS EMR cluster this is not required
try:
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
except:
    print("Could not set OS environment")


def create_spark_session():
    """
    Create a spark session or load from an existing spark session
    :return: SparkSession
    """
    logging.info("Creating spark session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def load_song_data(spark, input_song_data_path):
    """
    Load song data from the specified input_song_data_path
    :param spark: SparkSession
    :param input_song_data_path: path to the song data files
    :return: song dataframe
    """
    logging.info("Load song data")
    return spark.read.json(input_song_data_path)


def load_log_data(spark, input_log_data_path):
    """
    Load log data from the specified input_log_data_path
    :param spark: SparkSession
    :param input_log_data_path: path to the log data files
    :return: log dataframe
    """
    logging.info("Load log data")
    return spark.read.json(input_log_data_path)


def preprocess_log_data(log_data):
    """
    Filter log data which action is viewing a song, and convert ts to timestamp
    :param log_data: log dataframe
    :return: processed log dataframe
    """
    logging.info("Preprocess log data")
    # filter for page=NextSong only
    log_data = log_data.filter(log_data.page == "NextSong")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    log_data = log_data.withColumn("timestamp", get_timestamp("ts"))

    return log_data


def process_song_table(song_data, output_data_path):
    """
    Create song table and write it to the output_data_path
    :param song_data: song dataframe
    :param output_data_path: output path to store the song table
    :return: None
    """
    logging.info("Process songs table")
    # extract columns to create songs table
    songs_table = song_data.dropDuplicates(['song_id']).select("song_id", "title", "artist_id", "duration", "year")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        f"{output_data_path}/songs_table.parquet")


def process_artist_table(song_data, output_data_path):
    """
    Create artist table and write it to the output_data_path
    :param song_data: song dataframe
    :param output_data_path: output path to store the artist table
    :return: None
    """
    logging.info("Process artists table")
    # extract columns to create artists table
    artists_table = song_data.dropDuplicates(['artist_id']).select("artist_id", col("artist_name").alias("name"),
                                                                   col("artist_location").alias("location"),
                                                                   col("artist_latitude").alias("latitude"),
                                                                   col("artist_longitude").alias("longitude"))

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data_path}/artists_table.parquet")


def process_users_table(log_data, output_data_path):
    """
    Create users table and write it to the output_data_path
    :param log_data: log dataframe
    :param output_data_path: output path to store the users table
    :return: None
    """
    logging.info("Process users table")
    # extract columns for users table
    users_table = log_data.dropDuplicates(["userId"]).select(col("userId").alias("user_id"),
                                                             col("firstName").alias("first_name"),
                                                             col("lastName").alias("last_name"), "gender", "level")

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data_path}/users_table.parquet")


def process_time_table(log_data, output_data_path):
    """
    Create time table and write it to the output_data_path
    :param log_data: log dataframe
    :param output_data_path: output path to store the time table
    :return: None
    """
    logging.info("Process time table")
    # extract columns to create time table
    time_table = log_data.dropDuplicates(["ts"]).select(col("timestamp").alias("start_time"),
                                                        hour("timestamp").alias("hour"),
                                                        dayofmonth("timestamp").alias("day"),
                                                        weekofyear("timestamp").alias("week"),
                                                        month("timestamp").alias("month"),
                                                        year("timestamp").alias("year"),
                                                        dayofweek("timestamp").alias("weekday"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(f"{output_data_path}/time_table.parquet")


def process_songplays_table(song_data, log_data, output_data_path):
    """
    Create songplays table and write it to the output_data_path
    :param song_data: song dataframe
    :param log_data: log dataframe
    :param output_data_path: output path to store the songplays table
    :return: None
    """
    logging.info("Process songplays table")
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_data.join(song_data, (log_data.song == song_data.title) & (
            log_data.artist == song_data.artist_name)).select(monotonically_increasing_id().alias('songplay_id'),
                                                              col('timestamp').alias('start_time'),
                                                              year('timestamp').alias('year'),
                                                              month('timestamp').alias('month'),
                                                              col('userId').alias('user_id'), 'level', 'song_id',
                                                              'artist_id', col('sessionId').alias('session_id'),
                                                              'location', col('userAgent').alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        f"{output_data_path}/songplays_table.parquet")


def main():
    """
    Read the song and log data from SONG_DATA_PATH and LOG_DATA_PATH
    Transform the data into OLAP tables
    Write the tables to output DATA_PATH
    :return: None
    """
    # create Spark session
    spark = create_spark_session()

    # read the input and output path from config file
    input_song_data_path = "s3a://udacity-dend/song_data/A/A/*/*.json"
    input_log_data_path = "s3a://udacity-dend/log_data/*/*/*.json"

    # please put the s3 output data path here
    output_data_path = ""

    # load the input data
    song_data = load_song_data(spark, input_song_data_path)
    log_data = load_log_data(spark, input_log_data_path)

    # preprocess log_data
    log_data = preprocess_log_data(log_data)

    process_song_table(song_data, output_data_path)
    process_artist_table(song_data, output_data_path)
    process_users_table(log_data, output_data_path)
    process_time_table(log_data, output_data_path)
    process_songplays_table(song_data, log_data, output_data_path)


if __name__ == "__main__":
    main()
