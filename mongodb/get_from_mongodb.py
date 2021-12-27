import os
import pymongo
import argparse
import pandas as pd

from datetime import datetime
from bson.json_util import dumps

#mongo_uri = "mongodb://localhost:27017/"
#psth_to_save = '../data'
#db = 'openweather_db'
#collection = 'openweather'

def openweather_mdb_to_json(mongo_uri,
                            db,
                            collection):
    '''Get data from MongoDB and saves as json file.
    
    Params:
    -------
        mongo_uri - str: string connection to MongoDB.
        db - str: Mongo database to extract the data.
        connection - str: Database collection to extract the data.
        path_to_save - str: Path where the json file will be saved.
    
    Return:
    -------
        None
    '''

    # access to mongodb localy
    cl = pymongo.MongoClient(mongo_uri)
    # get database.collection
    ow_collection = cl[db][collection]
    # select all data
    data = ow_collection.find()
    # extract data to a list
    data_list = [x for x in data]

    # save as json
    # Converting to the JSON
    json_data = dumps(data_list) 

    today = datetime.today().date()
    path = '../data/data_lake/landing/'
    filename = f'openweather_{today}.json'.replace('-','')
    
     # create data folder if not exixts
    if not os.path.exists(path):
        #os.makedirs(path)
        print(f'{path} does not exists.')
        raise FileNotFoundError

    # Writing data to file openweather.json
    with open(f'{path}{filename}', 'w') as file:
        file.write(json_data)

    print(f'{filename} saved at {path}.')

    return None

def parse_args():
    parser = argparse.ArgumentParser(
        description = 'Get data from mongodb collection.'
    )

    parser.add_argument(
        '-m',
        '--mongouri',
        type=str,
        default="mongodb://localhost:27017/",
        help='<REQUIRED> Mongo URI.'
    )

    parser.add_argument(
        '-db',
        '--database',
        type=str,
        default='openweather_mdb',
        help='<REQUIRED> Mongo database to extract the data.'
    )

    parser.add_argument(
        '-c',
        '--collection',
        type=str,
        default='openweather',
        help='<REQUIRED> Collection to extract the data.'
    )

    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    mongo_uri = args.mongouri
    db = args.database
    collection = args.collection

    openweather_mdb_to_json(mongo_uri=mongo_uri,
                            db=db,
                            collection=collection)

if __name__=='__main__':
    main()