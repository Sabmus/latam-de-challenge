from typing import List, Tuple
import emoji
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql import functions as sf
from helpers.de_nest import extract_all_tweets
import re

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q2: Memory")

    # Carga de datos
    json_data = spark.load_json(file_path).unpersist()
    df = extract_all_tweets(json_data, "all_quoted").unpersist()

    emojis = sorted(emoji.EMOJI_DATA, key=len, reverse=True)
    pattern = '(' + '|'.join(re.escape(u) for u in emojis) + ')'

    # Extract emojis using regexp_extract
    df = df.withColumn("emojis", sf.regexp_extract_all(sf.col("content"), sf.lit(r""+pattern))).where(sf.size(sf.col("emojis")) > 0)
    df = df.select(sf.explode("emojis").alias("emoji"))

    top_10_emojis = df.groupBy("emoji") \
        .agg(sf.count("emoji").alias("emojiCount")) \
        .orderBy(sf.desc("emojiCount")) \
        .limit(10) \
        .collect()
    

    # termino ejecucion de spark
    spark.get_spark().catalog.clearCache()
    spark.stop()
    return [(row.emoji, row.emojiCount) for row in top_10_emojis]