from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, date_format, current_timestamp
import os
import time

# Kafka and MinIO configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPICS = ['flood-warnings', 'flood-areas', 'water-levels', 'stations', 'historical-readings']

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio_access_key')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio_secret_key')
MINIO_BUCKET = 'data-lakehouse'
BRONZE_FOLDER = 'bronze'

def create_spark_session():
    """Create a Spark session configured to work with Kafka and MinIO"""
    spark = SparkSession.builder \
        .appName("KafkaToMinIO") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    print("Spark session created successfully!")
    return spark

def process_kafka_topic(spark, topic):
    """Process a Kafka topic and save raw data to MinIO bronze layer"""
    print(f"Starting to process Kafka topic: {topic}")
    
    # Create a streaming DataFrame from Kafka source
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Convert the binary value to string and add ingestion timestamp
    # adding metadata for the bronze layer
    bronze_df = df \
        .selectExpr("CAST(value AS STRING) as data", "timestamp as kafka_timestamp") \
        .withColumn("ingest_timestamp", current_timestamp()) \
        .withColumn("date_str", date_format(current_timestamp(), "yyyy-MM-dd"))
    
    # Define the path for saving to bronze layer
    output_path = f"s3a://{MINIO_BUCKET}/{BRONZE_FOLDER}/{topic}"
    
    # Write the streaming data to MinIO bronze layer
    # Partition by date string for efficient storage and querying
    query = bronze_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", f"/tmp/checkpoint/{BRONZE_FOLDER}/{topic}") \
        .partitionBy("date_str") \
        .start()
    
    print(f"Successfully started streaming for topic {topic} to bronze layer at {output_path}")
    return query

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # Set timezone to UTC
    spark.sql("SET spark.sql.session.timeZone=UTC")
    
    # Start streaming for each topic, sending data to bronze layer
    print(f"Starting to stream data from Kafka to MinIO bronze layer for {len(KAFKA_TOPICS)} topics")
    queries = []
    for topic in KAFKA_TOPICS:
        query = process_kafka_topic(spark, topic)
        queries.append(query)
    
    print(f"All {len(queries)} streaming queries started successfully")
    print(f"Data is being saved to the bronze layer in MinIO bucket: {MINIO_BUCKET}/{BRONZE_FOLDER}")
    
    # Wait for all query terminations
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main() 

# Run with Kafka connector:
# docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 /opt/bitnami/spark/scripts/to_minio.py

# docker exec -it spark-master spark-submit /opt/bitnami/spark/scripts/to_minio.py