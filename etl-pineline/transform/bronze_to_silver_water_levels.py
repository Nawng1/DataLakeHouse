import json
from pyspark.sql import SparkSession, Row

# --- Cấu hình ---
process_date = "2025-05-25"
input_path = "/opt/bitnami/spark/transform/stations/part.2"
output_path = f"s3a://data-lakehouse/silver/stations/date={process_date}/"

# --- Khởi tạo Spark ---
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

# --- Đọc JSON nhúng trong file nhị phân ---
with open(input_path, "rb") as f:
    raw = f.read()

try:
    json_start = raw.find(b'{"data":')
    json_str = raw[json_start:].decode("utf-8")
    outer = json.loads(json_str)
    inner = json.loads(outer["data"])
    items = inner["items"]
except Exception as e:
    print(f"❌ Lỗi xử lý file: {e}")
    spark.stop()
    exit(1)

# --- Rút gọn field ---
fields = ["@id", "RLOIid", "catchmentName", "dateOpened", "easting", "label", "lat", "long",
          "northing", "notation", "riverName", "stationReference", "status", "town", "wiskiID"]

df = spark.createDataFrame([Row(**{k: i.get(k) for k in fields}) for i in items])

# --- Ghi ra Silver ---
df.write.format("delta").mode("overwrite").save(output_path)

spark.stop()
