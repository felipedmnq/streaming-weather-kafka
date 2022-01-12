'''Kafka Consumer - kafka -> MongoDB

Consume weather data from kafka to MongoDB.
'''

from kafka import KafkaConsumer
from pymongo import MongoClient
import json

def kafka_consumer(kfk_bootstrap_server:str, db_name:str) -> None:
    try:
        # connect to MongoDB
        client = MongoClient('localhost',27017)
        # select or create database
        db = client[db_name]
        print("Connected successfully!")
    except:  
        print("Could not connect to MongoDB")

    # Instance kafka consumer
    consumer = KafkaConsumer(
        'openweather',
        bootstrap_servers=kfk_bootstrap_server,
        enable_auto_commit=True,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    ) 
    # get list of databases from mongodb
    dblist = client.list_database_names()

    if db_name in dblist:
        # Parse received data from Kafka
        for msg in consumer:
            #json_data = json.loads(msg.value)
            # Create dictionary and ingest data into MongoDB
            msg = msg[6]
            try:
                input_dict = {
                    'created_at': msg['created_at'],
                    'city_id': msg['city_id'],
                    'city_name': msg['city_name'],
                    'lat': msg['lat'],
                    'lon': msg['lon'],
                    'country': msg['country'],
                    'temp': msg['temp'],
                    'max_temp': msg['max_temp'],
                    'min_temp': msg['min_temp'],
                    'feels_like': msg['feels_like'],
                    'humidity': msg['humidity']
                }
                # insert data into mongodb collection
                input_db = db.openweather.insert_one(input_dict)
                print(f"Data inserted with record ids {input_db}")
            except:
                print("Could not insert into MongoDB")
    else:
        print(f'{db_name} not found')

def main():
    kfk_bootstrap_server = 'localhost:9092'
    db_name = "openweather_mdb"
    topic = "openweather"
    kafka_consumer(kfk_bootstrap_server=kfk_bootstrap_server,
                   db_name=db_name)

if __name__=='__main__':
    main()
