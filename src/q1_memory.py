from typing import List, Tuple
from datetime import datetime
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql.types import DateType
from pyspark.sql import functions as sf 

# funcion para extraer los detalles de los tweets
def extract_details(df, quotedTweetLevel):
    return df.select(
        sf.col(f"{quotedTweetLevel}.id").alias("id"),
        sf.col(f"{quotedTweetLevel}.date").alias("date"),
        sf.col(f"{quotedTweetLevel}.user.username").alias("username")
    ).where(sf.col(f"{quotedTweetLevel}.id").isNotNull())

# funcion para extraer todos los tweets de los niveles nesteados
# de quotedTweet
def extract_all_tweets(json_data):
    # selecciono las columnas que me interesan para este análisis
    df = json_data.select("id", "date", sf.col("user.username").alias("username"))
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
        df = df.union(ndf).distinct()
        current_level += 1
    
    return df


@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Inicializacion de Spark
    spark = SparkClass("Q1: Memory")
    # Carga de datos
    json_data = spark.load_json(file_path)
    
    df = extract_all_tweets(json_data)

    # obtengo las top 10 fechas con mas tweets
    top_10_dates = df.groupBy(df["date"].cast(DateType()).alias("date")) \
        .agg(sf.count("id").alias("tweetCount")) \
        .orderBy(sf.desc("tweetCount")) \
        .limit(10)
    
    # filtro el df principal con las top 10 fechas para luego agrupar según cantidad de tweets por usuario
    top_user_by_date = df.filter(df.date.cast(DateType()).isin([row.date for row in top_10_dates.collect()])) \
        .groupBy(df["date"].cast(DateType()).alias("date"), "username") \
        .agg(sf.count("id").alias("tweetCount")).orderBy(sf.desc("tweetCount")) \
        .limit(10)
    
    # hago un join con ambos df para obtener el resultado final
    result = top_10_dates.join(top_user_by_date, 
               top_10_dates.date == top_user_by_date.date, 
               "inner").select(top_10_dates.date, top_user_by_date.username).orderBy(sf.desc(top_10_dates.tweetCount)).collect()

    # termino ejecucion de spark
    spark.get_spark().catalog.clearCache()
    spark.stop()
    return [(row.date, row.username) for row in result]