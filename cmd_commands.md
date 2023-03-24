# Start zookeeper
C:\kafka>.\bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# Start Kafka server
C:\kafka>.\bin\windows\kafka-server-start.bat config\server.properties

# Create topic
C:\kafka>.\bin\windows\kafka-topics.bat --create --topic PinterestData --bootstrap-server localhost:9092 --partitions 3

# Start producer
C:\kafka>.\bin\windows\kafka-console-producer.bat --topic PinterestData --bootstrap-server localhost:9092

# Start consumer
C:\kafka>.\bin\windows\kafka-console-consumer.bat --topic PinterestData --bootstrap-server localhost:9092 --from-beginning