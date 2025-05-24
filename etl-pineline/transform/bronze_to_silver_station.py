import sys
import json
from pyspark.sql import SparkSession, Row

# --- Lấy ngày xử lý ---
process_date = "2025-05-25"
input_path = f"/opt/bitnami/spark/transform/stations/part.2"
output_path = f"s3a://data-lakehouse/silver/stations/date=2025-05-25/"

# --- Khởi tạo SparkSession ---
spark = SparkSession.builder \
    .appName("ToSilver_Stations") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Đọc file nhị phân, trích xuất JSON ---
with open(input_path, "rb") as f:
    raw = f.read()

json_start = raw.find(b'{"data":')
json_str = raw[json_start:].decode("utf-8")
outer = json.loads(json_str)
inner = json.loads(outer["data"])
items = inner["items"]

# --- Các trường cần giữ lại ---
required_fields = [
    "@id", "RLOIid", "catchmentName", "dateOpened", "easting", "label", "lat", "long",
    "northing", "notation", "riverName", "stationReference", "status", "town", "wiskiID"
]

def normalize_station(item):
    return {key: item.get(key, None) for key in required_fields}

rows = [Row(**normalize_station(item)) for item in items]
df = spark.createDataFrame(rows)

# --- Ghi ra Silver layer ---
df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)