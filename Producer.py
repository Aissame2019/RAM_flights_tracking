import requests
from confluent_kafka import Producer
import json
import time

WEBSITE_URL ="https://airlabs.co/api/v9/flights?api_key=595a933f-71d6-4044-91a7-3e9522fa7554&airline_icao=RAM" 
#"https://airlabs.co/api/v9/flights?api_key=a44bed74-b633-4b94-afa1-8115adb946e7"
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "data"

def fetch_data():
    response = requests.get(WEBSITE_URL)
    if response.status_code == 200:
        response_data = response.json()
        return response_data.get("response")  # Extract the "response" data
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def produce_to_kafka(data, producer):
    data_json = json.dumps(data)
    producer.produce(KAFKA_TOPIC, value=data_json.encode())
    producer.flush()
    print("Data produced to Kafka")

if __name__ == '__main__':
    KAFKA_SERVER = "localhost:9092"

    producer = Producer({"bootstrap.servers": KAFKA_SERVER})

    while True:
        data = fetch_data()
        if data:
            if isinstance(data, list) and len(data) > 0:
                first_item = data[0:50]
                produce_to_kafka(first_item, producer)
            else:
                print("No data available")
        else:
            print("Failed to fetch data from the API")
        time.sleep(5)
