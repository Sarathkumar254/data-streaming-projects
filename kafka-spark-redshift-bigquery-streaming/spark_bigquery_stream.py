from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, IntegerType

# Package dependencies
kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
spark_bigquery_package = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"

guava_package = "/mnt/c/Users/PillagovulaK/Downloads/guava-30.1-jre.jar"
hadoop_package = "/mnt/c/Users/PillagovulaK/Downloads/gcs-connector-hadoop3-latest.jar"

spark = SparkSession.builder \
    .appName("PySpark Kafka to bigquery with Stateful Deduplication") \
    .config("spark.jars.packages", f"{kafka_package},{spark_bigquery_package}") \
    .config("spark.jars", f"{guava_package},{hadoop_package}") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/mnt/c/Users/PillagovulaK/Downloads/triple-bird-457617-k1-f8613d64524a.json") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .getOrCreate()


# Kafka Configuration
kafka_bootstrap_servers = 'localhost:9092'  # Replace with your Kafka server address
kafka_topic = 'telecom-data'

# Schema of Incoming Data
schema = StructType() \
    .add("caller_name", StringType()) \
    .add("receiver_name", StringType()) \
    .add("caller_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("start_datetime", StringType()) \
    .add("end_datetime", StringType()) \
    .add("call_duration", IntegerType()) \
    .add("network_provider", StringType()) \
    .add("total_amount", StringType())

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option('startingOffsets', 'latest') \
    .load()

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Data Quality Check (Example: Ensuring call_duration is positive)
df = df.filter(df.call_duration > 0)

# bigquery Configuration
bigquery_table = "triple-bird-457617-k1.users_data.telecom_data"

df.printSchema()
print("Streaming started !")
print("********************************")

def write_to_bigquery(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .option("table", bigquery_table) \
        .option("project", "triple-bird-457617-k1") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("writeDisposition", "WRITE_APPEND") \
        .option("parentProject", "triple-bird-457617-k1") \
        .option("checkpointLocation", "gs://tempbigquery123/checkpoints/") \
        .option("temporaryGcsBucket", "tempbigquery123")\
        .mode("append") \
        .save()

# Use foreachBatch to control the batch processing
df.writeStream \
    .foreachBatch(write_to_bigquery) \
    .outputMode("append") \
    .start() \
    .awaitTermination()