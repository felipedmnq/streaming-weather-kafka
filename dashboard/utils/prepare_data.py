from typing import Optional
from pyspark.sql import SparkSession

def createSession(master: Optional[str]="local[*]",app_name: Optional[str]="MyApp") -> SparkSession: 
    ''' Creates a Spark Session '''
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
    return spark




print(type(spark))

