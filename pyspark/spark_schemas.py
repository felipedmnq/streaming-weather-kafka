from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType

main_schema = StructType([
    StructField('_id', StringType(), False),
    StructField('created_at', TimestampType(), False),
    StructField('city_id', IntegerType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lon', DoubleType(), True),
    StructField('country', StringType(), True),
    StructField('temp', DoubleType(), True),
    StructField('max_temp', DoubleType(), True),
    StructField('min_temp', DoubleType(), True),
    StructField('feels_like', DoubleType(), True),
    StructField('humidity', IntegerType(), True)
])