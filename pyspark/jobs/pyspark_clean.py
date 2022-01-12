import json, os, re, sys, logging, shutil
from typing import Callable, Optional
from itertools import chain
from datetime import datetime
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, ArrayType


# find the project directory
ABS_FILE_PATH = os.path.abspath(__file__) # file absolute path
FILE_DIR = os.path.dirname(ABS_FILE_PATH) # file dir
PROJECT_DIR = os.path.dirname(FILE_DIR) # project dir
MAIN_DIR = os.path.dirname(PROJECT_DIR)
FILENAME = os.path.basename(__file__) # get the filenamecd

today = f'{datetime.today().date()}'.replace('-', '')   

# create log file
LOG_FILE = f"{PROJECT_DIR}/logs/job-{FILENAME}.log"

# set log format
LOG_FORMAT = f"%(asctime)s \
    -LINE: %(lineno)d \
    -%(name)s \
    -%(levelname)s \
    -%(funcName)s \
    -%(message)s"

# set logging configuration
logging.basicConfig(filename=LOG_FILE, \
                    level=logging.DEBUG, \
                    format=LOG_FORMAT)
logger = logging.getLogger('py4j')

# insert POJECT_DIR path into sys.path at position 0
# this way python can find where my packeges are installed
# and is able to import them without errors.
sys.path.insert(0, PROJECT_DIR)
from classes.pyspark_class import SparkClass
from schemas.spark_schemas import main_schema

def main(PROJECT_DIR:str) -> None:
    ''' Starts a Spark job '''
    # extract mostly recent data from mongodb
    #get_from_mongodb
    # get config file from json folder
    asd
    config = openFile(f"{PROJECT_DIR}/conf/spark_session_config.json")
    # start sparksession
    spark = sparkStart(config)
    # get most recent json file from landing layer
    filepath = f"{MAIN_DIR}/data/data_lake/landing/openweather_{today}.json"
    # create RDD
    df = read_json(spark, main_schema, filepath)
    df = clean_id(df)
    df = city_names(df)
    df = replace_country(df)
    df = kelvin_to_fahreheint(df, "temp")
    df = kelvin_to_celcius(df, "temp")
    df = extract_date(df)
    save_as_parquet(df)

def sparkStart(config:dict) -> SparkSession:
    '''Get or Create Spark Session
    
    Params:
    -------
        config - dict: json
    Return:
    -------
        SparkSession
    '''
    if isinstance(config, dict):
        return SparkClass(conf={}).startSpark(config)
    
def openFile(filepath:str) -> dict:
    
    def openJson(filepath:str) -> dict:
        # check if filepath is str and if the filepath exists.
        if isinstance(filepath, str) and os.path.exists(filepath): 
            with open(filepath, "r") as F:
                data = json.load(F)
            return data
    
    return openJson(filepath)


def read_json(spark:SparkSession, schema, json_path):
    '''Read json file.
    
    Params:
    -------
        schema: Struct object with table schema.
        json_path - str: Path to json file.
        
    Return:
    -------
        Data Frame.
    '''
    if isinstance(spark, SparkSession):
        return spark.read.schema(schema).json(json_path)

def clean_id(df):
    '''Clean id column
    
    Params:
    -------
        df: Data Frame to be cleaned.
    
    Return:
    -------
        data frame with column id cleaned.
    '''
    if isinstance(df, DataFrame):
        def extract(col):
            if col: return re.findall(r'"\d+\w+"', col)
            else: return None

        extract_udf = F.udf(lambda x: extract(x), ArrayType(StringType()))
        df_cleaned_id = df.withColumn('id', extract_udf(df._id)[0]).drop('_id')
    
    return df_cleaned_id


def city_names(df):
    '''Create new column with cities names.
    
    Return:
    -------
        Data Frame with "city" column. 
    '''
    cities_map = {
        "2950159": "Berlin",
        "2988507": "Paris",
        "3128760": "Barcelona",
        "2759794": "Amsterdam",
        "3094802": "Krakow",
        "2761369": "Vienna",
        "2643743": "London"
    }
    if isinstance(df, DataFrame):
        mapping_expr = F.create_map([F.lit(x) for x in chain(*cities_map.items())])
        df_cities = df.withColumn('city', mapping_expr.getItem(F.col("city_id")))
    
    return df_cities


def replace_country(df):
    '''Replace country abbreviation with the country name.
    '''
    country_map = {
       "NL": "Netherlands",
       "PL": "Poland",
       "AT": "Austria",
       "GB": "England",
       "DE": "Germany",
       "ES": "Spain",
       "FR": "France"
    }
    if isinstance(df, DataFrame):
        mapping_expr = F.create_map([F.lit(x) for x in chain(*country_map.items())])
        df_countries = df.withColumn('country', mapping_expr.getItem(F.col("country")))
    
    return df_countries


def kelvin_to_fahreheint(df, col, new_col_name=None):
    '''Converts the temperature from kelvin to Fahreheint and create a new column with the result.
    
    Params:
    -------
        df: Dataframe to perform the conversion.
        col - str: Column with Kelvin temperatures.
        new_col_name - str: New column name.
        
    Return:
    -------
        Dataframe with new fahreheint column.
    '''
    F_func = lambda x: (x - 273.15) * 9/5 + 32
    F_udf = F.udf(F_func, DoubleType())
    
    if isinstance(df, DataFrame):
        if new_col_name:
            df_fahrehenint = df.withColumn(f'{new_col_name}', F.round(F_udf(df[col]), 2))
        else:
            df_fahrehenint = df.withColumn(f'{col}_F', F.round(F_udf(df[col]), 2))
    
    return df_fahrehenint


def kelvin_to_celcius(df, col, new_col_name=None):
    '''Converts the temperature from kelvin to Celcius and create a new column with the result.
    
    Params:
    -------
        df: Dataframe to perform the conversion.
        col - str: Column with Kelvin temperatures.
        new_col_name - str: New column name.
        
    Return:
    -------
        Dataframe with new Celcius column.
    '''
    C_func = lambda x: x - 273.15
    C_udf = F.udf(C_func, DoubleType())

    if isinstance(df, DataFrame):
        if new_col_name:
            df_celcius = df.withColumn(new_col_name, F.round(C_udf(df[col]), 2))
        else:
            df_celcius = df.withColumn(f'{col}_C', F.round(C_udf(df[col]), 2))
            
    return df_celcius
    

def extract_date(df):
    '''Extract month, day or hour from "created_at"
    '''
    if isinstance(df, DataFrame):
        df_month = df.withColumn("month", F.month(df.created_at))
        df_day = df.withColumn("day", F.dayofmonth(df_month.created_at))
        df_hour = df.withColumn("hour", F.hour(df_day.created_at))
    return df_hour
        
    
def save_as_parquet(df, layer="cleansed"):
    '''Save dataframe as parquet.
    
    Params:
    -------
        df: Dataframe to be saved as parquet.
        layer - str: Data Lake layer where the parquet file will be saved.
            options: "landing", "cleansed", "trusted".
    '''
    # save as parquet
    output_dir = f'{MAIN_DIR}/data/data_lake/{layer}/'
    if layer == 'landing':
        filename = f'raw_openweather_{today}'
    if layer == 'cleansed':
        filename = f'clean_openweather_{today}'
    else:
        filename = f'trusted_openweather_{today}'
    file_path = f"{output_dir}{filename}.parquet"
     
    # check if parquet file already exists. IF yes, delete it and save a new one.
    # update parquet file
    if os.path.exists(file_path):
        shutil.rmtree(file_path, ignore_errors=True)
    if isinstance(df, DataFrame):
        df.write.parquet(file_path)
        print(f"{filename}.parquet saved at {output_dir}")
    
    return None

if __name__=="__main__":
    main(PROJECT_DIR)