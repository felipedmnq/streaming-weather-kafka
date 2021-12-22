'''Extract data from mongodb
'''

import os
import pymongo
import pandas as pd

from datetime import datetime
from bson.json_util import dumps

mongo_con = "mongodb://localhost:27017/"
folder_to_save = '../data'

def openweather_mdb_to_json(mongo_con, folder_to_save):
    # access to mongodb localy
    cl = pymongo.MongoClient(mongo_con)
    # get database.collection
    ow_collection = cl.openweather_mdb.openweather
    # select all data
    data = ow_collection.find()
    # extract data to a list
    data_list = [x for x in data]

    # save as json
    # Converting to the JSON
    json_data = dumps(data_list) 

    # create data folder if not exixts
    if not os.path.exists(folder_to_save):
        os.makedirs(folder_to_save)

    today = datetime.today().date()
    filename = f'openweather_{today}'.replace('-','')

    # Writing data to file openweather.json
    with open(f'{folder_to_save}/{filename}.json', 'w') as file:
        file.write(json_data)

    print(f'{filename} saved at {folder_to_save}.')

def main():
    openweather_mdb_to_json(mongo_con=mongo_con,folder_to_save=folder_to_save)

if __name__=='__main__':
    main()