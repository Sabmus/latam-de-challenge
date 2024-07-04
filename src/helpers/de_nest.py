from pyspark.sql import functions as sf

def all_quoted(json_data):
    return json_data.alias("df").drop("quotedTweet")

def q1_memo(json_data):
    return json_data.select("id", "date", sf.col("user.username").alias("username"))

def q2_memo(json_data):
    pass

def q3_memo(json_data):
    return json_data.select("id", "mentionedUsers")

options = {
    "all_quoted": all_quoted,
    "q1_memo": q1_memo,
    "q2_memo": q2_memo,
    "q3_memo": q3_memo
}

# funcion para extraer los quoted tweets
def extract_details(df, quotedTweetLevel, option):
    if option == "all_quoted":
        return df.select(sf.col(f"{quotedTweetLevel}.*"))
    if option == "q1_memo":
        return df.select(
            sf.col(f"{quotedTweetLevel}.id").alias("id"),
            sf.col(f"{quotedTweetLevel}.date").alias("date"),
            sf.col(f"{quotedTweetLevel}.user.username").alias("username")
        ).where(sf.col(f"{quotedTweetLevel}.id").isNotNull())
    if option == "q3_memo":
        return df.select(
            sf.col(f"{quotedTweetLevel}.id").alias("id"),
            sf.col(f"{quotedTweetLevel}.mentionedUsers").alias("mentionedUsers"),
        ).where(sf.col(f"{quotedTweetLevel}.id").isNotNull())

# funcion para extraer todos los tweets de los niveles nesteados
# de quotedTweet
def extract_all_tweets(json_data, option):
    df = options[option](json_data)
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
        ndf = extract_details(json_data, quoted_tweet_col, option)
        
        # uno al df principal
        # pyspark drop no raisea un error si no existe la columna
        # This is a no-op if the schema doesn’t contain the given column name(s).
        # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.drop.html
        df = df.union(ndf.drop("quotedTweet")).dropDuplicates(["id"])
        current_level += 1
    
    return df