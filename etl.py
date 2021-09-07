import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, row_number, desc, when, broadcast
from pyspark.sql.types import IntegerType,TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import  year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: This function creates the spark session which will be used in teh rest of the script.

        Arguments:
            None
        Returns:
            None
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This funciton processes the songs files on S3 and populate SONGS and ARTISTS folders

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output:data: S3 Output bucket which will be used to save the output folders ( SONGS, ARTISTS)

        Returns:
            None
    """

    # get filepath to song data file
    song_data = input_data + config['AWS']['SONG_FILES']
    
    # read song data file
    df = spark.read.schema("`song_id` STRING,`num_songs` INT,`title` STRING,`artist_name` STRING,`artist_latitude` DOUBLE,"+
                    "`year` INT,`duration` DOUBLE,`artist_id` STRING,`artist_longitude` DOUBLE,`artist_location` STRING")\
            .json(song_data)


    # extract columns to create songs table and remove duplicates based on latest year and largest duration
    latest_song = Window.partitionBy("song_id").orderBy(desc("year"), desc("duration"))
    songs_table = df.select(["song_id","title","artist_id","year","duration"])
    songs_table = songs_table.withColumn("R_RANK",row_number().over(latest_song))
    songs_table = songs_table.filter(songs_table["R_RANK"] == 1).select(["song_id","title","artist_id","year","duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite")\
                    .parquet(output_data+config["TABLES"]["SONGS"])


    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude","year"])\
                                .withColumnRenamed("artist_location","location")\
                                .withColumnRenamed("artist_latitude","latitude")\
                                .withColumnRenamed("artist_longitude","longitude")\
                                .withColumnRenamed("artist_name","name")

    # remove duplicates based on giving priority for location and latitude which are not null
    artists_table = artists_table.withColumn("OrderCol_1",when(artists_table["latitude"].isNull(),0).otherwise(1))
    artists_table = artists_table.withColumn("OrderCol_2",when(artists_table["location"].isNull(),0).otherwise(1))
    latest_artist = Window.partitionBy("artist_id").orderBy(desc("year"),desc("OrderCol_1"),desc("OrderCol_2"))
    artists_table = artists_table.withColumn("R_RANK",row_number().over(latest_artist))
    artists_table = artists_table.filter(artists_table["R_RANK"] == 1)\
        .select(["artist_id","name","location","latitude","longitude"])

    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
        .parquet(output_data+config["TABLES"]["ARTISTS"])




def process_log_data(spark, input_data, output_data):
    """
        Description: This funciton processes the logs files on S3 and populate USERS,TIME and SONGPLAYS folders

        Arguments:
            spark: spark session object
            input_data: S3 input bucket which has the files.
            output:data: S3 Output bucket which will be used to save the output folders ( SONGS, ARTISTS)

        Returns:
            None
    """

    # get filepath to log data file
    log_data = input_data + config['AWS']['LOG_FILES']

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
    users_table.write.mode("overwrite")\
        .parquet(output_data+config["TABLES"]["USERS"])


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts:datetime.fromtimestamp(ts/1000.0),TimestampType())
    df = df.withColumn("rtimestamp",get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.select(["ts", "rtimestamp"]).distinct() \
        .withColumnRenamed("ts", "start_time") \
        .withColumn("hour", hour(df["rtimestamp"])) \
        .withColumn("day", dayofmonth(df["rtimestamp"])) \
        .withColumn("week", weekofyear(df["rtimestamp"])) \
        .withColumn("month", month(df["rtimestamp"])) \
        .withColumn("year", year(df["rtimestamp"])) \
        .withColumn("weekday", date_format(df["rtimestamp"], "E")) \
        .select(["start_time", "hour", "day", "week", "month", "year", "weekday"]).distinct()

        # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").\
        parquet(output_data+config["TABLES"]["TIME"])


    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+config["TABLES"]["SONGS"])
    artist_df = spark.read.parquet(output_data + config["TABLES"]["ARTISTS"])

    # extract columns from joined song and log datasets to create songplays table 
    artist_df = broadcast(artist_df)
    song_df = broadcast(song_df)

    df.createOrReplaceTempView("logs")
    song_df.createOrReplaceTempView("songs")
    artist_df.createOrReplaceTempView("artists")
    spark.udf.register('get_timestamp',get_timestamp)

    songplays_table = spark.sql('''
    SELECT row_number() over(order by ts) songplay_id,
           ts start_time,
           userId user_id,
           level,
           song_id,
           artist_id,
           sessionId session_id,
           location,
           userAgent user_agent,
           year(rtimestamp)  year,
           month(rtimestamp) month
    FROM
    (
		select distinct ts,rtimestamp,userId,level,sessionId,location,userAgent,song,artist
		FROM logs lg
	) lg
	LEFT OUTER JOIN 
	(
	    select song_id,sng.artist_id,title,name
	    FROM songs sng
	    INNER JOIN artists art
	    ON sng.artist_id = art.artist_id
	) s_a
	ON lg.song = s_a.title AND lg.artist = s_a.name
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite")\
        .parquet(output_data+config["TABLES"]["SONGPLAYS"])





def main():
    spark = create_spark_session()
    input_data = config['AWS']['INPUT_BUCKET']
    output_data = config['AWS']['OUTPUT_BUCKET']


    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)



if __name__ == "__main__":
    main()
