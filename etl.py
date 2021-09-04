import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,row_number,desc
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import  year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + config['AWS']['SONG_FILES']
    
    # read song data file
    df = spark.read.schema("`song_id` STRING,`num_songs` INT,`title` STRING,`artist_name` STRING,`artist_latitude` DOUBLE,"+
                    "`year` INT,`duration` DOUBLE,`artist_id` STRING,`artist_longitude` DOUBLE,`artist_location` STRING")\
            .json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id")\
                    .parquet("s3a://"+config['AWS']['OUTPUT_BUCKET']+"/"+config["TABLES"]["SONGS"])

    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_location","artist_latitude","artist_longitude"])\
                                .withColumnRenamed("artist_location","location")\
                                .withColumnRenamed("artist_latitude","latitude")\
                                .withColumnRenamed("artist_longitude","longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet("s3a://"+config['AWS']['OUTPUT_BUCKET']+"/"+config["TABLES"]["ARTISTS"])


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + config['AWS']['SONG_FILES']

    # read log data file
    df = spark.read.schema("`artist` STRING,`auth` STRING,`firstName` STRING,`gender` STRING," +
                           "`itemInSession` INT,`lastName` STRING,`length` DOUBLE," +
                           "`level` STRING,`location` STRING,`method` STRING,`page` STRING," +
                           "`registration` DOUBLE,`sessionId` INT,`song` STRING,`status` SHORT," +
                           "`ts` LONG,`userAgent` STRING,`userId` STRING") \
            .json(log_data)

    df.withColumn("userId",df["userId"].cast(IntegerType()))
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    latestlevel = Window.partitionBy("userId").orderBy(desc("ts"))
    users_table = df.select(["userId","firstName","lastName","gender","level","ts"])
    users_table = users_table.filter(users_table["userId"].isNotNull()).withColumn("U_LATEST",row_number().over(latestlevel))
    users_table = users_table.filter(users_table["U_LATEST"] == 1).select(["userId","firstName","lastName","gender","level"])
    users_table = users_table.withColumnRenamed("userId","user_Id")\
        .withColumnRenamed("firstName","first_name")\
        .withColumnRenamed("lastName","last_name")

    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
