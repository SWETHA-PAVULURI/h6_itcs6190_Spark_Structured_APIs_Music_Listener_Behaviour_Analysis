# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
logs_df = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_df = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

df = logs_df.join(songs_df.select("song_id", "genre"), on="song_id", how="left")
df.show(5)

# Task 1: User Favorite Genres
user_genre_count = df.groupBy("user_id", "genre").agg(count("*").alias("play_count"))

window_user = Window.partitionBy("user_id")
user_fav_genre = user_genre_count.withColumn("max_play_count", max("play_count").over(window_user)) \
                                 .where(col("play_count") == col("max_play_count")) \
                                 .select("user_id", "genre", "play_count")

user_fav_genre.show(10)

user_fav_genre.write.mode("overwrite").format("csv").option("header", "true").save("output/user_favorite_genres")

# Task 2: Average Listen Time
avg_listen_time = df.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration_seconds"))
avg_listen_time.show(10)

avg_listen_time.write.mode("overwrite").format("csv").option("header", "true").save("output/avg_listen_time_per_song")

# Task 3: Genre Loyalty Scores
# Join favorite genre back to main df
df_with_fav = df.join(user_fav_genre.select("user_id", "genre").withColumnRenamed("genre", "fav_genre"), on="user_id")

loyalty = df_with_fav.withColumn("is_fav_genre", (col("genre") == col("fav_genre")).cast("integer")) \
                     .groupBy("user_id") \
                     .agg(avg("is_fav_genre").alias("loyalty_score"))

# Filter users with loyalty score > 0.8
loyal_users = loyalty.filter(col("loyalty_score") > 0.8)
loyal_users.show(10)

loyal_users.write.mode("overwrite").format("csv").option("header", "true").save("output/genre_loyalty_scores")

# Task 4: Identify users who listen between 12 AM and 5 AM
df_time = df.withColumn("hour", hour(col("timestamp")))

late_night_users = df_time.filter((col("hour") >= 0) & (col("hour") < 5)) \
                          .select("user_id").distinct()

late_night_users.show(10)

late_night_users.write.mode("overwrite").format("csv").option("header", "true").save("output/night_owl_users")