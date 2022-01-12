import os, sys, json
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, from_json 

# SET FOLDERS PATHS
ABS_FILE_PATH = os.path.abspath(__file__) # file absolute path
FILE_DIR = os.path.dirname(ABS_FILE_PATH) # file dir
PROJECT_DIR = os.path.dirname(FILE_DIR) # project dir

sys.path.insert(0, f"{PROJECT_DIR}/pyspark")
from schemas.spark_schemas import main_schema
#from jobs.pyspark_clean import clean_id, city_names, replace_country
#from jobs.pyspark_clean import kelvin_to_fahreheint, kelvin_to_celcius, extract_date

BTSTRAP_SERVER = "localhost:9092"
KAFKA_TOPIC_INPUT = "openweather"
KAFKA_TOPIC_OUTPUT = "streamingsink"

if __name__=="__main__":
    # INITIATE SPARKSESSION
    spark = SparkSession \
        .builder \
        .appName("pysparkstreaming") \
        .master("local[*]") \
        .getOrCreate()

    # DEFINE THE SCHEMA
    schema = main_schema
    
    # SET SPARK STREAMING 
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC_INPUT) \
        .option("startingOffsets", "latest") \
        .option("value_deserializer", lambda x: json.loads(x.decode('utf-8'))) \
        .load()

    df_value = df.selectExpr("CAST(value AS STRING)", "timestamp")

    stream_values = df_value \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BTSTRAP_SERVER) \
        .option("topic", KAFKA_TOPIC_OUTPUT) \
        .outputMode("append") \
        .option("checkpointLocation", f"{FILE_DIR}/checkpointLocation") \
        .start()

    stream_values.awaitTermination()

    
    
    