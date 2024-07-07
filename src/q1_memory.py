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
    df = spark.load_parquet(file_path).select("id", "username", "date")
    df = df.withColumn("date_only", sf.col("date").cast(DateType())) \
        .repartition(4, "date_only") \
        .drop("date").cache()

    # obtengo las top 10 fechas con mas tweets 
    top_10_dates = df.groupBy(df["date_only"].alias("date")) \
        .agg(sf.count("id").alias("tweetCount")) \
        .orderBy(sf.desc("tweetCount")) \
        .limit(10).cache()
    
    # filtro el df principal con las top 10 fechas para luego agrupar según cantidad de tweets por usuario
    filtro = df.date_only.isin([row.date for row in top_10_dates.take(10)])
    top_user_by_date = df.filter(filtro) \
        .groupBy(df["date_only"].alias("date"), "username") \
        .agg(sf.count("id").alias("tweetCount")).orderBy(sf.desc("tweetCount")) \
        .limit(10).cache()
    
    # libero memoria de df
    df.unpersist()
    
    # hago un join con ambos df para obtener el resultado final
    result = top_10_dates.alias("top_10_dates") \
            .join(top_user_by_date.alias("top_user_by_date"),top_10_dates.date == top_user_by_date.date, "inner") \
            .select("top_10_dates.date", "top_user_by_date.username") \
            .orderBy(sf.desc("top_10_dates.tweetCount")) \
            .take(10)
    
    # libero memoria de top_10_dates y top_user_by_date
    top_10_dates.unpersist()
    top_user_by_date.unpersist()

    # termino ejecucion de spark
    spark.stop_spark()
    return [(row.date, row.username) for row in result]