#!/opt/homebrew/bin/python3

import json, os, re, sys, logging
from typing import Callable, Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

# find the project directory
ABS_FILE_PATH = os.path.abspath(__file__) # file absolute path
FILE_DIR = os.path.dirname(ABS_FILE_PATH) # file dir
PROJECT_DIR = os.path.dirname(FILE_DIR) # project dir
FILENAME = os.path.basename(__file__) # get the filename

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
from classes import pyspark_class

def main(PROJECT_DIR:str) -> None:
    ''' Starts a Spark job '''
    # get config file from json folder
    config = openFile(f"{PROJECT_DIR}/conf/spark_session_config.json")
    spark = sparkStart(config)
    sparkStop(spark)

def openFile(filepath:str) -> dict:
    def openJson(filepath:str) -> dict:
        # check if filepath is str and if the filepath exists.
        if isinstance(filepath, str) and os.path.exists(filepath): 
            with open(filepath, "r") as F:
                data = json.load(F)
            return data
    return (openJson(filepath))

def sparkStart(config:dict) -> SparkSession:
    if isinstance(config, dict):
        return pyspark_class.SparkClass(conf={}).startSpark(config)

def sparkStop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark, SparkSession) else None

if __name__ == "__main__":
    main(PROJECT_DIR)
