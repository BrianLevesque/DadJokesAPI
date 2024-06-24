from confluent_kafka import Producer
import socket
import random
import time
import requests
import json

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':64535'

api_url = 'https://icanhazdadjoke.com/'

headers = {
    "Accept": "text/plain"
}


def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s' % err)
    else:
        print('%% Message delivered to %s [%d]' % (msg.topic(), msg.partition()))


def main():
    # Kafka producer configuration
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': socket.gethostname()}

    # Create Kafka producer instance
    producer = Producer(conf)
    
    
    while True:
        response = requests.get(api_url, headers=headers)
        joke =response.text
        producer.produce(KAFKA_TOPIC, value=str(joke), callback=delivery_callback)

        # Flush messages to Kafka to ensure they are sent immediately
        producer.flush()
        # Wait 5 seconds to loop again            
        time.sleep(5)

    # Close Kafka producer
    producer.close()
    
if __name__ == '__main__':
    main()
