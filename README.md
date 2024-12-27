# Simulate credit card transactions stream using Kafka and PySpark Streaming

## Tools Required: Apache Kafka, Apache Hadoop, Apache Spark, PySpark, PowerBI, Apache Airflow

## How to run

### Apache Hadoop
- Using `cmd`
  - Run `hdfs namenode -format`
  - Run `start-dfs.cmd` 
  - Run `start-yarn.cmd`

### Apache Kafka
- Using `cmd`
  - Run `C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties` to start Zookeeper server
  - Run `C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties` to start Kafka server
  - (Create topic Project if haven't) Run `C:\kafka\bin\windows\kafka-topics.bat --create --topic Project --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`
  - Run `C:\kafka\bin\windows\kafka-console-consumer.bat --topic Project --bootstrap-server localhost:9092 --from-beginning` to start a consumer
- Using `VS Code`
  - Run all in `KafkaProducer.ipynb` to start ingesting data

 ### Apache Spark
 - Using `cmd`
  - Run `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 [Your path to SparkStreaming.py]`

### Accessing stored data
- Open your browser, go to `localhost:9870` (Based on your Hadoop config)
- In `Utilities` tab, select `Browse Local Files`
- `Transactions` folder contains data as `.csv` and `checkpoints` folder

### Visualization using PowerBI
- After running Kafka, Hadoop, Spark, open file Power BI to see the report
- The default source is `http://localhost:9870/webhdfs/v1`
- To change the source, go to Transform data. Go to Home -> Advanced Editor. Then change the Source by replacing the port based on your Hadoop config. Then Close & Apply
- To access the latest data, go to Home -> Refresh

### Apache Airflow
- In Airflow folder, copy `powerbi_refresh_dag.py` and paste it in `../airflow/dags` (Airflow installation folder)
- Make sure to change the path to your `refresh_power_bi.ps1` in `powerbi_refresh_dag.py` and path to your `PBIDesktop.exe` in `refresh_power_bi.ps1`
- Open `WSL`
- Run `source airflow_env/bin/activate ` to activate virtual environment
- Run `airflow scheduler`
- Open another `WSL` window and activate virtual environment
- Run `airflow webserver ` to run webserver
- You can access homepage through `localhost:8080` and activate it
