from typing import List, Tuple
import emoji
from spark_class import SparkClass
from pyspark.sql import functions as sf
import re

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q2: Time")

    # Carga de datos
    df = spark.load_parquet(file_path).select("content").where(sf.col("content").isNotNull()).cache()

    short_pattern = '([\u0900-\u097F]+|[\x00-\x7F]+)'
    content = df.select(sf.regexp_replace(sf.col('content'), fr'{short_pattern}', '').alias("content")) \
        .filter(sf.col("content") != "").cache()
    
    df.unpersist()
    
    emojis = sorted(emoji.EMOJI_DATA, key=len, reverse=True) 
    pattern = '(' + '|'.join(re.escape(u) for u in emojis) + ')'

    # Extract emojis using regexp_extract
    df2 = content.withColumn("emojis", sf.expr(f"regexp_extract_all(content, r'{pattern}')")) \
        .where(sf.size(sf.col("emojis")) > 0) \
        .drop("content") \
        .cache()
    
    content.unpersist()
    
    df3 = df2.select(sf.explode("emojis").alias("emoji")).cache()

    df2.unpersist()
    
    top_10_emojis = df3.groupBy("emoji") \
        .agg(sf.count("emoji").alias("emojiCount")) \
        .orderBy(sf.desc("emojiCount")) \
        .take(10)
    
    df3.unpersist()

    # termino ejecucion de spark
    spark.get_spark().catalog.clearCache()
    spark.stop()
    return [(row.emoji, row.emojiCount) for row in top_10_emojis]