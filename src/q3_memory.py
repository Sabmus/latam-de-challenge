from typing import List, Tuple
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql import functions as sf 

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q3: Memory")
    # Carga de datos
    df = spark.load_parquet(file_path).select("id", "mentionUser").cache()

    # hago un explode de los mentionedUsers para abrir el array y luego tomo solo el nombre de usuario
    df = df.select(sf.explode("mentionUser").alias("username")).select("username")

    # obtengo las top 10 fechas con mas tweets
    top_10_users = df.groupBy("username") \
        .agg(sf.count("username").alias("mentionCount")) \
        .orderBy(sf.desc("mentionCount")) \
        .take(10)
    
    # libero memoria de df
    df.unpersist()
    
    # termino ejecucion de spark
    spark.stop_spark()
    return [(row.username, row.mentionCount) for row in top_10_users]