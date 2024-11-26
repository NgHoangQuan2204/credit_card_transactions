# Simulate credit card transactions stream using Kafka and PySpark Streaming

## Apps Required: Apache Kafka, Apache Hadoop, Apache Spark, PySpark

## How to run
### Apache Kafka
- Using cmd
  - Run `C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties`
  - Run `C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties`
  - (Create topic Project if haven't) Run `C:\kafka\bin\windows\kafka-topics.bat --create --topic Project --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
  - Run `C:\kafka\bin\windows\kafka-console-consumer.bat --topic Project --bootstrap-server localhost:9092 --from-beginning`
- Using VS Code
  - Run all in `KafkaProducer.ipynb`

### Apache Hadoop
- Using cmd
  - Run `hdfs namenode -format`
  - Run `start-dfs.cmd` 
  - Run `start-yarn.cmd`

 ### Apache Spark
 - Using cmd
  - Run `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 [Your path to SparkStreaming.py]`

### Accessing stored data
- Open your browser, go to `localhost:9870` (Based on your Hadoop config)
- In `Utilities` tab, select `Browse Local Files`
- `Transactions` folder contains data as `.csv` and `checkpoints` folder
