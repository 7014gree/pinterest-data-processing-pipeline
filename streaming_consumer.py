from kafka import KafkaConsumer
from json import loads

test1_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

test1_consumer.subscribe(topics=["PinterestData"])

print("test") 
try:
    for message in test1_consumer:
        print(message.value)
except KeyboardInterrupt:
    print("ended")