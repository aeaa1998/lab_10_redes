from kafka import KafkaProducer
from constants import KAFKA_TOPIC, wind_options
import random
import json
import time


kafka_producer = KafkaProducer(bootstrap_servers='20.120.14.159:9092')


while(True):

    temperature = round(random.uniform(0.0, 100.0),2)
    temperature_restricted = int(round(random.uniform(0.0, 100.0),2))

    humidity = random.randint(0, 100)

    wind_direction = random.choice(wind_options)
    wind_direction_restricted = wind_options.index(wind_direction)
    payload = {'temperature': temperature, 'humidity': humidity, 'wind direction': wind_direction}
    payload_dump = json.dumps(payload)
    # Making it compatible to three bytes
    payload_restricted = chr(temperature_restricted) + chr(humidity) + chr(wind_direction_restricted)

    payload_restricted_bytes = payload_restricted.encode('ASCII')

    kafka_producer.send(KAFKA_TOPIC, payload_restricted_bytes)

    random_time = random.randint(2, 5)
    time.sleep(random_time)




