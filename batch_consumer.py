from kafka import KafkaConsumer
from json import loads, dumps
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
        file_name = f"msg3{message.timestamp}.json"

        # old: writes json to folder then reads and uploads
        """with open(file_name, 'w') as f:
            dump(message.value, f)
        s3_client.upload_file(file_name, 'pinterest-data-1c1ab27f-1210-48c6-a2b1-c24fb0286f93', file_name)"""

        # new: doesn't write file to folder first, only uploads data to s3 bucket       
        s3_client.put_object(
            Body=dumps(message.value),
            Bucket='pinterest-data-1c1ab27f-1210-48c6-a2b1-c24fb0286f93',
            Key=file_name
        )
        print(f"Successfully uploaded: {file_name}\nMessage: {message.value}\n")
except KeyboardInterrupt:
    print("ended")