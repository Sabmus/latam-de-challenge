import os
from src.helpers import de_nest
from src import spark_class
from pyspark.sql import functions as sf 

file_path = "farmers-protest-tweets-2021-2-4.json"
data_folder_path = os.path.abspath(os.path.join(os.getcwd(), 'data'))

def json_to_parquet(json_path):
    spark = spark_class.SparkClass("Json to Parquet")
    json_data = spark.load_json(json_path)
    main_df = de_nest.extract_all_tweets(json_data, "all_quoted")
    print(main_df.count())

    df = main_df \
        .withColumn("username", sf.col("user.username")) \
        .withColumn("mentionUser", sf.expr("transform(mentionedUsers, x -> x.username)")) \
        .drop("media", "user", "mentionedUsers")
    
    print(df.printSchema())
    print(df.count())
    
    spark.save_as_parquet(df, f"{data_folder_path}\\tweets.parquet")
    spark.stop()

def check_parquet(ruta):
    spark = spark_class.SparkClass("test")
    parquet_df = spark.load_parquet(ruta)
    print(parquet_df.printSchema())
    print(parquet_df.count())
    spark.stop()


json_to_parquet(f"{data_folder_path}\\{file_path}")
#check_parquet(f"{data_folder_path}\\tweets.parquet")
