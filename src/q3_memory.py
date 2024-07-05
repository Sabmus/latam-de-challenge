from typing import List, Tuple
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql import functions as sf 

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q3: Memory")
    # Carga de datos
    #json_data = spark.load_json(file_path)
    #df = extract_all_tweets(json_data, "q3_memo")
    df = spark.load_parquet(file_path).select("id", "mentionUser")

    # hago un explode de los mentionedUsers para abrir el array y luego tomo solo el nombre de usuario
    df = df.select(sf.explode("mentionUser").alias("username")).select("username")
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