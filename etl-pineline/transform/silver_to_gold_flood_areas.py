from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, initcap, current_timestamp

# --- Khởi tạo SparkSession với Delta Lake ---
spark = SparkSession.builder \
    .appName("ToGold_FloodAreas") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Đường dẫn lớp Silver và Gold ---
silver_path = "s3a://data-lakehouse/silver/flood-areas/date=2025-05-22/"
gold_path = "s3a://data-lakehouse/gold/flood-areas/"

# --- Đọc dữ liệu Delta từ lớp Silver ---
df = spark.read.format("delta").load(silver_path)

# --- Làm sạch dữ liệu và chuẩn hóa ---
df_cleaned = df.select(
    df["@id"].alias("id"),
    trim("notation").alias("notation"),
    trim(initcap("label")).alias("label"),
    trim(initcap("county")).alias("county"),
    trim(initcap("eaAreaName")).alias("ea_area_name"),
    trim(initcap("floodWatchArea")).alias("flood_watch_area"),
    trim("fwdCode").alias("fwd_code"),
    "lat", "long",
    trim("polygon").alias("polygon"),
    trim("quickDialNumber").alias("quick_dial_number"),
    trim(initcap("riverOrSea")).alias("river_or_sea"),
    trim("description").alias("description"),
    current_timestamp().alias("ingested_at")
)

# --- Ghi dữ liệu ra lớp Gold ở định dạng Delta ---
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print("✅ Silver to Gold for flood-areas completed.")
