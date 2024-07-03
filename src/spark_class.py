from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

master: str = "local[*]"
max_cores: int = 1
memory: str = "2g"
max_partition_bytes: int = 128 * 1024 * 1024 # 128MB

class SparkClass:
    def __init__(self, app_name: str, master: str = master, max_cores: int = max_cores, memory: str = memory, max_partition_bytes: int = max_partition_bytes):
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster(master) \
            .set("spark.executor.cores", max_cores) \
            .set("spark.executor.memory", memory) \
            .set("spark.sql.files.maxPartitionBytes", max_partition_bytes)

        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    def load_json(self, file_path: str):
        return self.spark.read.json(file_path)

    def get_spark(self):
        return self.spark

    def stop(self):
        self.spark.stop()
