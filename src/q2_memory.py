from typing import List, Tuple
import emoji
from spark_class import SparkClass
from memory_profiler import profile
from pyspark.sql import functions as sf
import re
from pyspark.sql.types import ArrayType, StringType

# funcion para extraer los detalles de los tweets
def extract_details(df, quotedTweetLevel):
    return df.select(
        sf.col(f"{quotedTweetLevel}.id").alias("id"),
        sf.col(f"{quotedTweetLevel}.content").alias("content"),
    ).where(sf.col(f"{quotedTweetLevel}.id").isNotNull()).where(sf.col(f"{quotedTweetLevel}.content").isNotNull())

# funcion para extraer todos los tweets de los niveles nesteados
# de quotedTweet
def extract_all_content(json_data):
    # selecciono las columnas que me interesan para este análisis
    df = json_data.select("id", "content")
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

# Function to extract emojis using emoji_list
def extract_emojis(text):
    return [match['emoji'] for match in emoji.emoji_list(text)]

#@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    # Inicializacion de Spark
    spark = SparkClass("Q1: Memory")
    # Carga de datos
    json_data = spark.load_json(file_path)
    df = extract_all_content(json_data)
    print(df.printSchema())

    # rdd = df.rdd.map(lambda row: (extract_emojis(row['content'])))

    content = df.select("content").collect()

    # Process each row to extract emojis and create a new list of rows
    processed_data = [extract_emojis(row['content']) for row in content]

    # Create a new DataFrame with the additional emojis column
    new_df = spark.get_spark().createDataFrame(processed_data, ["emojis"])
    new_df = new_df.groupBy("emojis").count().orderBy(sf.desc("count")).limit(10)

    print(new_df.show(10))

    '''# List of emojis from the emoji library
    emoji_list = list(emoji.UNICODE_EMOJI['en'].keys())
    # Convert the emoji list to a string format suitable for regex
    emoji_regex = "[" + "".join(emoji_list) + "]"

    # Create a SQL expression to extract emojis
    extract_emojis_expr = f"regexp_extract_all(content, '{emoji_regex}')"
    # Apply the SQL expression to create a new column
    df_with_emojis = df.selectExpr("content", extract_emojis_expr + " as emojis")'''


    #df = df.withColumn("emojis", sf.array([sf.lit(emoji["emoji"]) for emoji in emoji_list(sf.col("content").cast(StringType()))]))

    #print(new_df.show(10))

    # termino ejecucion de spark
    spark.get_spark().catalog.clearCache()
    spark.stop()
    return []