from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

master: str = "local[*]" # uses all cores
max_cores: int = 2
memory: str = "4g"
max_partition_bytes: int = (100 * 1024 * 1024)

class SparkClass:
    def __init__(self, app_name: str, master: str = master, max_cores: int = max_cores, memory: str = memory, max_partition_bytes: int = max_partition_bytes):
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster(master) \
            .set("spark.executor.cores", max_cores) \
            .set("spark.executor.memory", memory) \
            .set("spark.driver.cores", max_cores) \
            .set("spark.driver.memory", "4g") \
            .set("spark.executor.memoryOverhead", "1g") \
            .set("spark.driver.maxResultSize", "3g") \
            .set("spark.sql.files.maxPartitionBytes", max_partition_bytes) \
            .set("spark.files.maxPartitionBytes", max_partition_bytes) \
            .set("spark.network.timeout", "240s") \
            .set("spark.network.timeoutInterval", "120s") \
            .set("spark.sql.files.minPartitionNum", 4) \

        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    def load_json(self, file_path: str):
        return self.spark.read.json(file_path)

    def get_spark(self):
        return self.spark

    def stop(self):
        self.spark.stop()
