from typing import List, Tuple
from spark_class import SparkClass
from pyspark.sql import functions as sf 

# funcion para extraer los detalles de los tweets
def extract_details(df, quotedTweetLevel):
    return df.select(sf.col(f"{quotedTweetLevel}.*"))

# funcion para extraer todos los tweets de los niveles nesteados
# de quotedTweet
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
        ndf = extract_details(json_data, quoted_tweet_col)

        # uno al df principal
        df = df.union(ndf.drop("quotedTweet")).dropDuplicates(["id"])
        current_level += 1
    
    return df

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q1: Memory")
    # Carga de datos
    json_data = spark.load_json(file_path)
    df = extract_all_tweets(json_data)
    # hago un explode de los mentionedUsers para abrir el array y luego tomo solo el nombre de usuario
    df = df.select(sf.explode("mentionedUsers").alias("mentionedUsersExploded")).select("mentionedUsersExploded.username")

    # obtengo las top 10 fechas con mas tweets
    top_10_users = df.groupBy("username") \
        .agg(sf.count("username").alias("mentionCount")) \
        .orderBy(sf.desc("mentionCount")) \
        .limit(10) \
        .collect()
    
    # termino ejecucion de spark
    spark.get_spark().catalog.clearCache()
    spark.stop()
    return [(row.username, row.mentionCount) for row in top_10_users]