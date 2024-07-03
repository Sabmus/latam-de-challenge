from typing import List, Tuple
from datetime import datetime
from spark_class import SparkClass
from memory_profiler import profile

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Inicializacion de Spark
    spark = SparkClass("Q1: Memory")
    # Carga de datos
    json_data = spark.load_json(file_path)

    
    # termino ejecucion de spark
    spark.stop()
    return []