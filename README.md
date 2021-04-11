# KAFKA_V2I_PROTOTYPE


To run kafka on **Windows**, 

* Zookeeper

> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

* Kafka

> .\bin\windows\kafka-server-start.sh .\config\server.properties

* Creating topics

> .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DEMO_TOPIC_1

> .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DEMO_TOPIC_2

> .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DEMO_TOPIC_3

> .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DEMO_TOPIC_4

> .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DEMO_TOPIC_5