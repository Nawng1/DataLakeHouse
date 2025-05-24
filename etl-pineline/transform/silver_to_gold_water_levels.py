from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim

spark = SparkSession.builder \
    .appName("ToGold_WaterLevels") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn
silver_path = "s3a://data-lakehouse/silver/water-levels/date=2025-05-25/"
gold_path = "s3a://data-lakehouse/gold/water-levels/"

# Đọc dữ liệu từ silver (delta)
df = spark.read.format("delta").load(silver_path)

# Lọc/chuẩn hóa các cột cần thiết
df_cleaned = df.select(
    col("@id").alias("id"),
    col("dateTime"),
    col("value"),
    col("measure"),
    current_timestamp().alias("ingested_at")
)

# Ghi ra lớp Gold (Delta Lake)
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)
