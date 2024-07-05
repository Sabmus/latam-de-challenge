from helpers import de_nest
import spark_class
from pyspark.sql import functions as sf 

def json_to_parquet(json_path, parquet_path):
    spark = spark_class.SparkClass("Json to Parquet")
    json_data = spark.load_json(json_path)
    main_df = de_nest.extract_all_tweets(json_data, "all_quoted")

    df = main_df \
        .withColumn("username", sf.col("user.username")) \
        .withColumn("mentionUser", sf.expr("transform(mentionedUsers, x -> x.username)")) \
        .drop("media", "user", "mentionedUsers")
    
    spark.save_as_parquet(df, f"{parquet_path}")
    print("Json to Parquet conversion completed successfully.")
    spark.stop()
