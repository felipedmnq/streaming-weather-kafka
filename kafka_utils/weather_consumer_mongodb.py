from kafka import KafkaConsumer
from pymongo import MongoClient
import json

kfk_bootstrap_server = 'localhost:9092'
db_name = "openweather_mdb"
topic = "openweather"

try:
   client = MongoClient('localhost',27017)
   db = client[db_name]
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")

consumer = KafkaConsumer(
    'openweather',
    bootstrap_servers=kfk_bootstrap_server,
    enable_auto_commit=True,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
) 

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
            
            input_db = db.openweather.insert_one(input_dict)
            print(f"Data inserted with record ids {input_db}")
        except:
            print("Could not insert into MongoDB")
else:
    print(f'{db_name} not found')
