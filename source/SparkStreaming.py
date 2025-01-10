from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, udf
from pyspark.sql.types import StructType, StringType, DoubleType
import pyspark.sql.functions as F
import pandas as pd

spark = SparkSession.builder \
    .appName("CreditCard") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.catalog.clearCache()

schema = StructType() \
    .add("User", StringType()) \
    .add("Card", StringType()) \
    .add("Year", StringType()) \
    .add("Month", StringType()) \
    .add("Day", StringType()) \
    .add("Time", StringType()) \
    .add("Amount", StringType()) \
    .add("Use Chip", StringType()) \
    .add("Merchant Name", StringType()) \
    .add("Merchant City", StringType()) \
    .add("Merchant State", StringType()) \
    .add("Zip", StringType()) \
    .add("MCC", StringType()) \
    .add("Errors?", StringType()) \
    .add("Is Fraud?", StringType())
    
source_df = (
    spark \
    .readStream \
    .format('kafka') \
    .options(**{
        'subscribe': 'Project',
        'startingOffsets': 'latest',
    }) \
    .option("kafka.bootstrap.servers", 'localhost:9092') \
    .load()
)

value_df = source_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

line = value_df.filter(col('Is Fraud?') == 'No') \
                .withColumn("AmountVND", col("Amount") * lit(24000)) \
                .withColumn("Date", F.date_format(F.unix_timestamp(F.make_date(col("Year"), col("Month"), col("Day"))).cast("timestamp"),"dd/MM/yyyy")) \
                .withColumn("Time", F.date_format("Time", "HH:mm:ss")) \
                .drop(col('Year')) \
                .drop(col('Month')) \
                .drop(col('Day'))

query = line.writeStream \
    .format("csv") \
    .trigger(processingTime="10 seconds") \
    .option("header",True) \
    .option("checkpointLocation", "hdfs://localhost:9000/Transactions/checkpoints") \
    .option("path", "hdfs://localhost:9000/Transactions") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

spark.catalog.clearCache()
spark.stop()