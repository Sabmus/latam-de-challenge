from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

master: str = "local[*]" # uses all cores
spark_max_cores: int = 4
executor_cores: int = 2
executor_memory: str = "4g"
driver_cores: int = 4
driver_memory: str = "4g"
# max_partition_bytes
# 38 mb tomando en cuenta el peso del archivo parquet, partido en 4 particiones
max_partition_bytes: int = (38 * 1024 * 1024) // 4
maxResultSize: str = "1g"
memoryOverhead: str = "1g"
network_timeout: str = "240s"
network_timeoutInterval: str = "120s"
sql_files_minPartitionNum: int = 4
sql_debu_maxToStringFields: int = 50
parquet_binaryAsString: bool = True

class SparkClass:
    def __init__(self, app_name: str, master: str = master, executor_cores: int = executor_cores, executor_memory: str = executor_memory, max_partition_bytes: int = max_partition_bytes):
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster(master) \
            .set("spark.cores.max", spark_max_cores) \
            .set("spark.executor.cores", executor_cores) \
            .set("spark.executor.memory", executor_memory) \
            .set("spark.driver.cores", driver_cores) \
            .set("spark.driver.memory", driver_memory) \
            .set("spark.executor.memoryOverhead", memoryOverhead) \
            .set("spark.driver.maxResultSize", maxResultSize) \
            .set("spark.sql.files.maxPartitionBytes", max_partition_bytes) \
            .set("spark.files.maxPartitionBytes", max_partition_bytes) \
            .set("spark.network.timeout", network_timeout) \
            .set("spark.network.timeoutInterval", network_timeoutInterval) \
            .set("spark.sql.files.minPartitionNum", sql_files_minPartitionNum) \
            .set("spark.sql.debug.maxToStringFields", sql_debu_maxToStringFields) \
            .set("spark.sql.parquet.binaryAsString", parquet_binaryAsString) \
            
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    def load_json(self, file_path: str):
        return self.spark.read.json(file_path)
    
    def save_as_parquet(self, df, data_path: str):
        pandas_df = df.toPandas()
        pandas_df.to_parquet(data_path, compression='snappy')

    def load_parquet(self, file_path: str):
        return self.spark.read.parquet(file_path)

    def get_spark(self):
        return self.spark

    def stop(self):
        self.spark.stop()
