from typing import List, Tuple
from datetime import datetime
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql.types import DateType
from pyspark.sql import functions as sf 

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Inicializacion de Spark
    spark = SparkClass("Q1: Memory")
    # Carga de datos
    json_data = spark.load_json(file_path)

    # TODO: ARREGLAR ESTO
    quoted1 = json_data.select("quotedTweet.*")
    quoted2 = quoted1.select("quotedTweet.*")
    quoted3 = quoted2.select("quotedTweet.*").select("date", "id", "user.username")

    df = json_data.select("date", "id", "user.username") \
        .union(quoted1.select("date", "id", "user.username")) \
        .union(quoted2.select("date", "id", "user.username")) \
        .union(quoted3.select("date", "id", "username")) \
        .distinct()

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