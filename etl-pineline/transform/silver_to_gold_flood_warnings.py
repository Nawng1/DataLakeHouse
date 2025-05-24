from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim
from datetime import datetime

# Tạo SparkSession với Delta Lake configs
spark = SparkSession.builder \
    .appName("ToGold_FloodWarnings") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn lớp silver và gold
silver_path = "s3a://data-lakehouse/silver/flood-warnings/date=2025-05-22"
gold_path = "s3a://data-lakehouse/gold/flood-warnings"

# Đọc dữ liệu từ silver layer
df = spark.read.format("delta").load(silver_path)

# Làm sạch dữ liệu
df_cleaned = df.select(
    trim(col("@id")).alias("id"),
    trim(col("description")).alias("description"),
    trim(col("eaAreaName")).alias("ea_area_name"),
    trim(col("floodAreaID")).alias("flood_area_id"),
    col("isTidal").cast("boolean"),
    trim(col("message")).alias("message"),
    trim(col("severity")).alias("severity"),
    col("severityLevel").cast("integer").alias("severity_level"),
    col("timeMessageChanged").cast("timestamp").alias("time_message_changed"),
    col("timeRaised").cast("timestamp").alias("time_raised"),
    col("timeSeverityChanged").cast("timestamp").alias("time_severity_changed"),
    current_timestamp().alias("ingested_at")
)

# Ghi ra lớp gold KHÔNG phân vùng
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_path)

spark.stop()
