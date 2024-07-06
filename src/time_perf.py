import os
from q1_time import q1_time
from q2_time import q2_time
from q3_time import q3_time

# declaro nombres de archivo y rutas
parquet_file = "tweets.parquet"
data_folder_path = os.path.abspath(os.path.join(os.getcwd(), '..', 'data'))
parquet_path = f"{data_folder_path}\\{parquet_file}"

def run_q1_time():
    q1_time(f"{parquet_path}")

def run_q2_time():
    q2_time(f"{parquet_path}")

def run_q3_time():
    q3_time(f"{parquet_path}")

run_q1_time()