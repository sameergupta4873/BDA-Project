# Real-Time Big Data Analytics Pipeline

## Stack:
- Apache Kafka (data ingestion)
- Apache Spark Streaming (real-time processing)
- Apache HBase & Hive (storage & querying)
- Apache Pig (batch processing)
- Apache Zookeeper (coordination)

## Steps to Run:
1. Start Zookeeper: `zkServer.sh start`
2. Start Kafka server and topic: `kafka-server-start.sh` + `kafka-topics.sh`
3. Run Kafka Producer: `python kafka_producer.py`
4. Run Spark Consumer: `spark-submit SparkKafkaConsumer.scala`
5. Create HBase table: `create_hbase_table.sh`
6. Load Hive table: `hive -f hive_table.hql`
7. Run Pig: `pig process_data.pig`

## Output
- Processed data stored in HBase
- SQL queries via Hive
- Aggregations via Pig
