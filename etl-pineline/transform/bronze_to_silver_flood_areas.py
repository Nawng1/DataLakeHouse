import sys
import json
from pyspark.sql import SparkSession, Row

# --- Lấy ngày xử lý ---
process_date = sys.argv[1] if len(sys.argv) > 1 else "2025-05-22"
input_path = f"/opt/bitnami/spark/transform/date_str={process_date}/part.1"
output_path = f"s3a://data-lakehouse/silver/flood-areas/date={process_date}/"

# --- Khởi tạo SparkSession với config MinIO + Delta ---
spark = SparkSession.builder \
    .appName("ToSilver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Đọc file nhị phân, trích xuất dữ liệu JSON ---
with open(input_path, "rb") as f:
    raw = f.read()

json_start = raw.find(b'{"data":')
json_str = raw[json_start:].decode("utf-8")
outer = json.loads(json_str)
inner_data = json.loads(outer["data"])
items = inner_data["items"]

# --- Chuẩn hóa dữ liệu để tránh thiếu field ---
required_fields = [
    "@id", "county", "description", "eaAreaName", "floodWatchArea",
    "fwdCode", "label", "lat", "long", "notation",
    "polygon", "quickDialNumber", "riverOrSea"
]

def normalize_item(item):
    return {k: item.get(k, None) for k in required_fields}

rows = [Row(**normalize_item(item)) for item in items]
spark_df = spark.createDataFrame(rows)

# --- Ghi ra Silver Layer (Delta Format) ---
spark_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)
