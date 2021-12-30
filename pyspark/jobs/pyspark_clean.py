import re, os
import shutil
from itertools import chain
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def start_spark_session(appName, master) -> SparkSession:
    '''Get or Create Spark Session
    
    Params:
    -------
        appName - str: Sets a name for the application, which will be shown in the Spark web UI.
            If no application name is set, a randomly generated name will be used.
            
        master - str: Sets the Spark master URL to connect to, such as “local” to run locally, 
            “local[4]” to run locally with 4 cores, or “spark://master:7077” to run on a Spark standalone cluster.
    Return:
    -------
        SparkSession
    '''
    return SparkSession.builder.appName(appName).master(master).getOrCreate()


def read_json(schema, json_path):
    '''Read json file.
    
    Params:
    -------
        schema: Struct object with table schema.
        json_path - str: Path to json file.
        
    Return:
    -------
        Data Frame.
    '''
    return pyspark.read.schema(schema).json(json_path)

def clean_id(df):
    '''Clean id column
    
    Params:
    -------
        df: Data Frame to be cleaned.
    
    Return:
    -------
        data frame with column id cleaned.
    '''
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
    
    if new_col_name:
        df_fahrehenint = df.withColumn(f'{new_col_name}', F.round(F_udf(df_countries[col]), 2))
    else:
        df_fahrehenint = df.withColumn(f'{col}_F', F.round(F_udf(df_countries[col]), 2))
    
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
    
    if new_col_name:
        df_celcius = df_fahrehenint.withColumn(new_col_name, F.round(C_udf(df_countries[col]), 2))
    else:
        df_celcius = df_fahrehenint.withColumn(f'{col}_C', F.round(C_udf(df_countries[col]), 2))
        
    return df_celcius
    

def extract_date(df, param):
    '''Extract month, day or hour from "created_at"
    '''
    if param == "month": 
        df_month = df.withColumn("month", F.month(df.created_at))
        return df_month
    if param == "day": 
        df_day = df.withColumn("day", F.dayofmonth(df_month.created_at))
        return df_day
    if param == "hour": 
        df_hour = df.withColumn("hour", F.hour(df_day.created_at))
        return df_hour
        
    
def save_as_parquet(df, layer):
    '''Save dataframe as parquet.
    
    Params:
    -------
        df: Dataframe to be saved as parquet.
        layer - str: Data Lake layer where the parquet file will be saved.
            options: "landing", "cleansed", "trusted".
    '''
    # save as parquet
    if layer not in ["landing", "cleansed", "trusted"]:
        raise FileNotFoundError('Selected data lake layer does not exists, please use "landing", "cleased" or "trusted".')
        
    today = f'{datetime.today().date()}'.replace('-', '')   
    output_dir = f'../data/data_lake/{layer}/'
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

    df.write.parquet(file_path)
    
    return None