from kafka import KafkaConsumer
from json import loads, dump
import boto3

s3_client = boto3.client('s3')


test1_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

test1_consumer.subscribe(topics=["Test_topic1"])

try:
    for message in test1_consumer:
        file_name = f"msg{message.timestamp}.json"
        with open(file_name, 'w') as f:
            dump(message.value, f)
            s3_client.upload_file(file_name, 'pinterest-data-1c1ab27f-1210-48c6-a2b1-c24fb0286f93', file_name)
except KeyboardInterrupt:
    print("ended")