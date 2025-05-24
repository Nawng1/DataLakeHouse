from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, initcap, current_timestamp

# --- Khởi tạo SparkSession với cấu hình Delta Lake ---
spark = SparkSession.builder \
    .appName("ToGold_Stations") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Đường dẫn Silver và Gold ---
silver_path = "s3a://data-lakehouse/silver/stations/date=2025-05-25/"
gold_path = "s3a://data-lakehouse/gold/stations/"

# --- Đọc Delta Table từ Silver ---
df = spark.read.format("delta").load(silver_path)

# --- Làm sạch và chuẩn hóa dữ liệu ---
df_cleaned = df.select(
    trim("@id").alias("id"),
    trim("stationReference").alias("station_reference"),
    trim(initcap("label")).alias("label"),
    trim("notation").alias("notation"),
    trim(initcap("town")).alias("town"),
    trim(initcap("riverName")).alias("river_name"),
    trim("RLOIid").alias("rloi_id"),
    trim("catchmentName").alias("catchment_name"),
    trim("wiskiID").alias("wiski_id"),
    "easting", "northing", "lat", "long",
    trim("status").alias("status"),
    trim("dateOpened").alias("date_opened"),
    current_timestamp().alias("ingested_at")
)

# --- Ghi ra lớp Gold theo định dạng Delta ---
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("✅ Silver to Gold for stations completed.")
