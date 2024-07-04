from typing import List, Tuple
from spark_class import SparkClass
from pyspark.sql import functions as sf
from helpers.de_nest import extract_all_tweets

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q1: Memory")
    # Carga de datos
    json_data = spark.load_json(file_path)
    df = extract_all_tweets(json_data, "all_quoted")
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