import os, sys
import pandas as pd
import streamlit as st
from PIL import Image
from datetime import datetime

NOW = datetime.now()
ABS_FILE_PATH = os.path.abspath(__file__) # file absolute path
FILE_DIR = os.path.dirname(ABS_FILE_PATH) # file dir
PROJECT_DIR = os.path.dirname(FILE_DIR) # project dir
MAIN_DIR = os.path.dirname(PROJECT_DIR)
DATA_DIR = f"{MAIN_DIR}/data/data_lake"

# insert PROJECT DIR path to sys.
sys.path.insert(0, PROJECT_DIR)
#from utils.prepare_data import *
sys.path.insert(0, MAIN_DIR)
from get_mongodb.get_from_mongodb import openweather_mdb_to_json
sys.path.insert(1, f"{MAIN_DIR}/pyspark")
from jobs.pyspark_clean import *

# DEFINING CONTAINERS
header = st.container()
sidebar = st.container()
dashboard = st.container()
get_data = st.container()

with sidebar:
    with header:
        logo = Image.open(os.path.join("..", "static", "logo_white_cropped.png"))
        st.sidebar.image(logo)

    st.sidebar.header("Get Data")

    with get_data:
        # walks cleansed folder and return the latest parquet file
        latest_file = [*os.walk(f"{DATA_DIR}/cleansed")][0][1][-2]
        file_path = f"{DATA_DIR}/cleansed/{latest_file}"
        #spark = createSession(app_name="ST Prepare Data")

        #data  = spark.read.parquet(file_path)
        # CONVERT TO PANDAS
        #data = data.toPandas()
        #st.write(type(data))
        #st.write(data.head())
        """
        for now the data is not being showed in Streamlit dashboard, why?
        check it!!
        #### AFTER PREPARE WATH IS NEEDED, NEEDS TO CHANGE RDD TO PANDAS DF.
        """

        last_update = f"LAST UPDATE: {NOW.date()} - {NOW.time().strftime('%H:%M:%S')}"
        st.sidebar.write(last_update)
        update_help = "Get up to date data."
        update_button = st.sidebar.button("Update", help=update_help)
        if update_button:
            last_update = last_update
            # RUN GET_FROM_MONGODB.PY
            mongo_uri = "mongodb://localhost:27017/"
            db = 'openweather_db'
            collection = 'openweather'
            path=f"{DATA_DIR}/landing"
            openweather_mdb_to_json(mongo_uri=mongo_uri, db=db, collection=collection, path_to_save=path)

            # RUN CLEANING - PYSPARK
            config = openFile(f"{PROJECT_DIR}/conf/spark_session_config.json")
            # start sparksession
            spark = sparkStart(config)
            # get most recent json file from landing layer
            #filepath = f"{MAIN_DIR}/data/data_lake/landing/openweather_{today}.json"
            last_json_filepath = [*os.walk(f"{MAIN_DIR}/data/data_lake/landing")][0][-1][-1]
            filepath = f"{MAIN_DIR}/data/data_lake/landing/{last_json_filepath}"
            st.write(filepath)
            # create RDD
            df = read_json(spark, main_schema, filepath)
            df = clean_id(df)
            df = city_names(df)
            df = replace_country(df)
            df = kelvin_to_fahreheint(df, "temp")
            df = kelvin_to_celcius(df, "temp")
            df = extract_date(df)
            save_as_parquet(df)


