from functools import cache
import os, sys
import pandas as pd
import streamlit as st
from PIL import Image
from datetime import datetime
from pandas import DataFrame
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.figure import Figure


st.set_page_config(layout="wide")

NOW = datetime.now()
ABS_FILE_PATH = os.path.abspath(__file__) # file absolute path
FILE_DIR = os.path.dirname(ABS_FILE_PATH) # file dir
PROJECT_DIR = os.path.dirname(FILE_DIR) # project dir
MAIN_DIR = os.path.dirname(PROJECT_DIR)
DATA_DIR = f"{MAIN_DIR}/data/data_lake"

# insert PROJECT DIR path to sys.
sys.path.insert(0, PROJECT_DIR)
#from utils.prepare_data import *
sys.path.insert(1, MAIN_DIR)
from get_mongodb.get_from_mongodb import openweather_mdb_to_json
sys.path.insert(2, f"{MAIN_DIR}/pyspark")
from jobs.pyspark_clean import *

# UPDATE DATA
def up_to_date(df: DataFrame) -> DataFrame:

    df["date"] = df["created_at"].apply(lambda date: date.strftime("%Y-%m-%d"))
    df["date"] = pd.to_datetime(df["date"])
    df = df.groupby(["date", "hour", "city"])[["temp_F", "temp_C", "humidity"]].mean().reset_index()
    last_date = max(df["date"])
    last_hour = max(df["hour"])
    df = df[df["date"] == last_date].reset_index(drop=True)
    return df

# GROUP CITIES BY DATE
def group_data(df: DataFrame) -> DataFrame:    
    df["date"] = df["created_at"].apply(lambda date: date.strftime("%Y-%m-%d"))
    df["date"] = pd.to_datetime(df["date"])
    df = df.groupby(["hour", "date", "city"])[["temp_F", "temp_C", "humidity"]].mean().reset_index()
    df = df.sort_values(by=["date", "hour"]).tail(7)
    return df

# LINEPLOT - TEMPERATURE BY DAY BY CITY
def line_plot_by_day(df: DataFrame, city: str) -> Figure:
    df3 = df[df["city"] == f"{city}"]
    fig = plt.figure(figsize=(6, 2))
    sns.lineplot(x=df3["hour"], y=df3["temp_C"], markers='o')
    plt.xticks(rotation=30)
    plt.grid()
    plt.title(f"Average Temerature by hour - {city}")
    plt.ylabel("Temperature - C")
    plt.xticks(range(1,24))
    return fig

@cache
def get_data() -> DataFrame:
    config = openFile(f"{PROJECT_DIR}/conf/spark_session_config.json")
    # start sparksession
    spark = sparkStart(config)
    # get most recent json file from landing layer
    last_json_filepath = [*os.walk(f"{MAIN_DIR}/data/data_lake/landing")][0][-1][-1]
    filepath = f"{MAIN_DIR}/data/data_lake/landing/{last_json_filepath}"
    # Clean json - save as parquet.
    df = read_json(spark, main_schema, filepath)
    df = clean_id(df)
    df = city_names(df)
    df = replace_country(df)
    df = kelvin_to_fahreheint(df, "temp")
    df = kelvin_to_celcius(df, "temp")
    df = extract_date(df)
    save_as_parquet(df)
    df = df.toPandas()
    df = df.drop(columns=['temp', 'max_temp', 'min_temp', 'feels_like', 'id', 'city_id'])
    df = df[['created_at', 'hour', 'country', 'city', 'lat', 'lon', 'temp_F', 'temp_C', 'humidity']]
    return df

def plot_temp(df):
    pass

def plot_map(df):
    pass

# DEFINING CONTAINERS
header = st.container()
sidebar = st.container()
dashboard = st.container()

# SET INITIAL DATAFRAME
DF = get_data()

with sidebar:
    with header:
        logo = Image.open(os.path.join("..", "static", "logo_white_cropped.png"))
        st.sidebar.image(logo)

    st.sidebar.header("Get Data")

    last_update = f"LAST UPDATE: {NOW.date()} - {NOW.time().strftime('%H:%M:%S')}"
    st.sidebar.write(last_update)
    update_help = "Get up to date data."
    update_button = st.sidebar.button("Update", help=update_help)
    #if update_button:
    last_update = last_update

    # SELECT SCOPE
    all_cities_help = 'Check this box to select all cities.'
    all_cities = st.sidebar.checkbox('Select All Cities', help = all_cities_help)
    if not all_cities:
        city_help = "Select the city to display the chart."
        city = st.sidebar.selectbox(label="City", options=DF["city"].unique(), help=city_help)

with dashboard:
    # walks cleansed folder and return the latest parquet file
    latest_file = [*os.walk(f"{DATA_DIR}/cleansed")][0][1][-2]
    file_path = f"{DATA_DIR}/cleansed/{latest_file}"

    # Update data from mongo db
    mongo_uri = "mongodb://localhost:27017/"
    db = 'openweather_db'
    collection = 'openweather'
    path=f"{DATA_DIR}/landing"
    openweather_mdb_to_json(mongo_uri=mongo_uri, db=db, collection=collection, path_to_save=path)

    block1 = st.container()
    block2 = st.container()

    with block1:
        data = group_data(DF)
        data = data.drop(columns=["hour", "date"])
        st.header("Weather Table")
        st.write(data)

    with block2:
        st.header("Graph and Map")
        col1, col2 = st.columns(2)
        with col1:
            dfg = group_data(DF)
            st.pyplot(line_plot_by_day(dfg, city=city))

        #with col2:
            #st.line_chart(line_plot_by_day(dfg, city=city))
            #st.map(df)


