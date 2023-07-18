import requests, json, random, socket, struct
# from bson import json_util

from kafka import KafkaProducer
from datetime import datetime

# Creating a Kafka producer instance
producer = KafkaProducer(bootstrap_servers='localhost:9092')

url_list = ['https://google.com', 'https://yahoo.com', 'https://youtube.com', 'https://internet.com', 'https://hello.com',
            'https://new.com', 'https://head.com', 'https://tail.com']

os_list = ['windows', 'ubuntu', 'linux', 'redhat', 'Android', 'IoS']
browser_list = ['chrome', 'firefox', 'safari', 'explorer']
device_list = ['Windows 10', 'Iphone12', 'Iphone13', 'BlackBerry', "Samsung"]

def get_location(ip_address): 
    response = requests.get(f'https://ipapi.co/{ip_address}/json/').json()
    location_data = {
        "city": response.get("city"),
        "country": response.get("country_name")
    }
    return location_data

# Publish data to a Kafka topic
def publish_message(producer, topic_name, value):
    producer.send(topic_name, value)
    producer.flush()

topic = 'test'

for i in  range(100):
    print(socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff))))
    location_data = get_location(socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff))))
    print('location_data:', location_data)
    data = { 'click_data ': {
                    'user_id':i, 
                    'timestamp': str(datetime.now()), 
                    'url': random.choice(url_list)},
            'geo_data' : {
                'user_country': location_data['country'],
                'city': location_data['city']},
            'user_agent_data': {
                'browser': random.choice(browser_list), 
                'OS': random.choice(os_list), 
                'device': random.choice(device_list)}
            }
    publish_message(producer, topic, json.dumps(data).encode('utf-8'))