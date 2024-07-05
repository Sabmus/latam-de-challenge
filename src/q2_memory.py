from typing import List, Tuple
import emoji
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql import functions as sf
from helpers.de_nest import extract_all_tweets
import re

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q2: Memory")

    # Carga de datos
    #json_data = spark.load_json(file_path).unpersist()
    #df = extract_all_tweets(json_data, "q2_memo").unpersist()
    #print(df.printSchema())
    df = spark.load_parquet(file_path).select("content").cache()

    emojis = sorted(emoji.EMOJI_DATA, key=len, reverse=True)
    pattern = '(' + '|'.join(re.escape(u) for u in emojis) + ')'

    # Extract emojis using regexp_extract
    df2 = df.withColumn("emojis", sf.regexp_extract_all(sf.col("content"), sf.lit(r""+pattern))) \
        .where(sf.size(sf.col("emojis")) > 0) \
        .drop("content") \
        .cache()

    df.unpersist()
    df3 = df2.select(sf.explode("emojis").alias("emoji")).cache()
    df2.unpersist()

    top_10_emojis = df3.groupBy("emoji") \
        .agg(sf.count("emoji").alias("emojiCount")) \
        .orderBy(sf.desc("emojiCount")).cache()
    
    result = top_10_emojis.take(10)
    top_10_emojis.unpersist()

    # termino ejecucion de spark
    spark.get_spark().catalog.clearCache()
    spark.stop()
    return [(row.emoji, row.emojiCount) for row in result]