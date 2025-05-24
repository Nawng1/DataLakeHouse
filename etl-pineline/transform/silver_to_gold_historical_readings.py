from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Tạo SparkSession với Delta extension
spark = SparkSession.builder \
    .appName("ToGold_HistoricalReadings") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn input/output
silver_path = "s3a://data-lakehouse/silver/historical-readings/date=2025-05-25/"
gold_path = "s3a://data-lakehouse/gold/historical-readings/"

# Đọc từ Silver Delta Table
df = spark.read.format("delta").load(silver_path)

# Xử lý và chuẩn hóa schema nếu cần
df_cleaned = df.select(
    "dateTime", "measure", "@id", "value"
).withColumnRenamed("@id", "reading_id") \
 .withColumn("ingested_at", current_timestamp())


# Ghi ra Gold Layer theo định dạng Delta
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_path)
