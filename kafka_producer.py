from kafka import KafkaProducer
from json import dumps



test1_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="test1_producer",
    value_serializer=lambda inputmessage: dumps(inputmessage).encode("ascii"),
)

topic_name = "Test_topic1"

while True:
    input_key = input("Enter key name: ")
    input_value = input("Enter value name: ")
    msg = {input_key: input_value}
    test1_producer.send(topic=topic_name, value=msg)

