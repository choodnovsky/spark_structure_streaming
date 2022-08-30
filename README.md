1. cd kafka_2.12-2.8.0
2. ./bin/zookeeper-server-start.sh config/zookeeper.properties
3. ./bin/kafka-server-start.sh config/server.properties
4. ./bin/kafka-topics.sh --create --topic netology --bootstrap-server localhost:9092
5. ./bin/kafka-console-producer.sh --topic netology --bootstrap-server localhost:9092
6. ./bin/kafka-console-consumer.sh --topic netology --from-beginning --bootstrap-server localhost:9092 
7. spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 ~/Documents/spark_streaming_hw/streaming_1.py