from kafka import KafkaConsumer, KafkaProducer
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata
from kafka.admin import NewTopic
from json import dumps, loads

meta_cluster_conn = ClusterMetadata(
    bootstrap_servers="localhost:9092",
)

print(meta_cluster_conn.brokers())

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="Kafka Administrator",

)

# Create and add topic
"""topics = []
topics.append(NewTopic(name="Test_topic1", num_partitions=2, replication_factor=1))

admin_client.create_topics(new_topics=topics)"""

print(admin_client.list_topics())
print(admin_client.describe_topics(topics=["Test_topic1"]))


# Creates producer, sends messages to topic Test_topic1
"""test1_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="test1_producer",
    value_serializer=lambda inputmessage: dumps(inputmessage).encode("ascii"),
)


input_message = [
    {
        "key1": "value9",
        "key2": "value10",
    },
    {
        "key1": "value11",
        "key2": "value12",
    }
]

for msg in input_message:
    test1_producer.send(topic="Test_topic1", value=msg)
"""


# Creates consumer, subscribes to Test_topic1, prints all messages from Test_topic1 and prints new ones as they are loaded
"""test1_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

test1_consumer.subscribe(topics=["Test_topic1"])

for message in test1_consumer:
    print(message.value)"""