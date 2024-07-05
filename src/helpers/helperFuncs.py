import sys
sys.path.append('src')
from spark_class import SparkClass
from pyspark.sql import functions as sf

# funcion para extraer todos los tweets de los 
# niveles nesteados de quotedTweet
def extract_all_tweets(json_data):
    df = json_data.alias("df").drop("quotedTweet")
    # como existen varios niveles de quotedTweet, se debe iterar para obtener todos los tweets
    current_level = 1
    while True:
        quoted_tweet_col = "quotedTweet" + ".quotedTweet" * (current_level - 1)
        next_level_col = quoted_tweet_col + ".quotedTweet"
        # checkeo si existe el siguiente nivel
        json_data = json_data.withColumn("has_next_level", sf.col(next_level_col).isNotNull())
        # si no existe el siguiente nivel, termino la iteración
        if json_data.filter(sf.col("has_next_level")).count() == 0:
            break
        # uso la función para extraer los detalles de los tweets
        ndf = json_data.select(sf.col(f"{quoted_tweet_col}.*"))
        
        # uno al df principal
        # pyspark drop no raisea un error si no existe la columna
        # This is a no-op if the schema doesn’t contain the given column name(s).
        # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html
        df = df.union(ndf.drop("quotedTweet")).dropDuplicates(["id"])
        current_level += 1
    
    return df

def json_to_parquet(json_path, parquet_path):
    spark = SparkClass("Json to Parquet")
    json_data = spark.load_json(json_path)
    main_df = extract_all_tweets(json_data)

    df = main_df \
        .withColumn("username", sf.col("user.username")) \
        .withColumn("mentionUser", sf.expr("transform(mentionedUsers, x -> x.username)")) \
        .drop("media", "user", "mentionedUsers")
    
    spark.save_as_parquet(df, f"{parquet_path}")
    print("Json to Parquet conversion completed successfully.")
    spark.stop()
