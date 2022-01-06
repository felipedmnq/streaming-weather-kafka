'''Kafka Producer

Extract weather data from OpenWeather API (https://openweathermap.org/api) to kafka.
'''

import time
import json
import requests
from config import config
from kafka import KafkaProducer

#kfk_bootstrap_server = 'localhost:9092'
kfk_bootstrap_server = '127.0.0.1:9092'

def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[kfk_bootstrap_server],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def get_weather_infos(openweather_endpoint:str) -> dict:
    '''Request the data from OpenWeather API

    Params:
    -------
        openweather_endpoint - str: 
            OpenWeather API endpoint.

    Retunr:
    -------
        json_msg: return the message to be send to kafka.
    '''
    api_response = requests.get(openweather_endpoint)
    json_data = api_response.json()
    city_id = json_data['id']
    city_name = json_data['name']
    lat = json_data['coord']['lat']
    lon = json_data['coord']['lon']
    country = json_data['sys']['country']
    temp = json_data['main']['temp']
    max_temp = json_data['main']['temp_max']
    min_temp = json_data['main']['temp_min']
    feels_like = json_data['main']['feels_like']
    humidity = json_data['main']['humidity']

    json_msg = {
        'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'city_id': city_id,
        'city_name': city_name,
        'lat': lat,
        'lon': lon,
        'country': country,
        'temp': temp,
        'max_temp': max_temp,
        'min_temp': min_temp,
        'feels_like': feels_like,
        'humidity': humidity
    }
    return json_msg

def main():
    kfk_topic = 'openweather'
    api_key = config()
    cities = ('London', 'Berlin', 'Paris', 'Barcelona', 'Amsterdam', 'Krakow', 'Vienna')
    while True:
        for city in cities:
            openweather_endpoint = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
            # units=metric -> return values in celcius
            #openweather_endpoint = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
            json_msg = get_weather_infos(openweather_endpoint)
            producer = kafka_producer()
            if isinstance(producer, KafkaProducer):
                producer.send(kfk_topic, json_msg)
                print(f'Published {city}: {json.dumps(json_msg)}')
                sleep = 60
                print(f'Whaiting {sleep} seconds ...')
                time.sleep(sleep)

if __name__=="__main__":
    main()
