from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

pyspark = SparkSession.builder.appName('OpenWeather').master('local[*]').getOrCreate()

schema = StructType([
    StructField('_id', StringType(), True),
    StructField('created_at', TimestampType(), True),
    StructField('city_id', IntegerType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lon', DoubleType(), True),
    StructField('country', StringType(), True),
    StructField('temp', DoubleType(), True),
    StructField('max_temp', DoubleType(), True),
    StructField('min_temp', DoubleType(), True),
    StructField('feels_like', DoubleType(), True),
    StructField('humidity', IntegerType(), True)]
)

today = f'{datetime.today().date()}'.replace('-', '')

df_pyspark_schema = pyspark.read.schema(schema).json(f'../data/openweather_{today}.json')
print(df_pyspark_schema.printSchema())
print(df_pyspark_schema.show())