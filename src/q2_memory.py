from typing import List, Tuple
import emoji
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql import functions as sf
import re

def find_all_emojis(text: str) -> List[str]:
    return emoji.emoji_list(text)

@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q2: Memory")
    # Carga de datos
    df = spark.load_parquet(file_path).select("content").where(sf.col("content").isNotNull()).cache()
    
    # creo patron de emojis
    # https://carpedm20.github.io/emoji/docs/index.html#regular-expression
    emojis = sorted(emoji.EMOJI_DATA, key=len, reverse=True) 
    pattern = '(' + '|'.join(re.escape(u) for u in emojis) + ')'

    # quise hacerlo con rdd pero no pude convertirlo a dataframe
    # 'PipelinedRDD' object has no attribute 'toDF' in PySpark
    # https://stackoverflow.com/questions/32788387/pipelinedrdd-object-has-no-attribute-todf-in-pyspark
    #rdd = df.rdd.map(lambda x: (find_all_emojis(x.content))).toDF(["content"])

    # patrón para eliminar casi todo lo que no sea emoji
    # con esto la idea es reducir el tamaño de "content", que tenga menos 
    # caracteres a procesar
    short_pattern = '([\u0900-\u097F]+|[\x00-\x7F]+)'
    content = df.select(sf.regexp_replace(sf.col('content'), fr'{short_pattern}', '').alias("content")) \
        .filter(sf.col("content") != "").cache()
    
    df.unpersist()
    
    # extraigo emojis usando expresión regular y el patrón creado
    df2 = content.withColumn("emojis", sf.expr(f"regexp_extract_all(content, r'{pattern}')")) \
        .where(sf.size(sf.col("emojis")) > 0) \
        .drop("content") \
        .cache()
    
    content.unpersist()
    
    # uso explode para "abrir" cada lista de emojis
    df3 = df2.select(sf.explode("emojis").alias("emoji")).cache()

    df2.unpersist()

    top_10_emojis = df3.groupBy("emoji") \
        .agg(sf.count("emoji").alias("emojiCount")) \
        .orderBy(sf.desc("emojiCount")) \
        .take(10)
    
    df3.unpersist()

    # termino ejecucion de spark
    spark.stop_spark()
    return [(row.emoji, row.emojiCount) for row in top_10_emojis]