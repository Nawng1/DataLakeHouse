import json
from pyspark.sql import SparkSession, Row

process_date = "2025-05-25"
input_path = "/opt/bitnami/spark/transform/historical-readings/part.3"
output_path = f"s3a://data-lakehouse/silver/historical-readings/date={process_date}/"

# Tạo Spark session
spark = SparkSession.builder \
    .appName("ToSilver_HistoricalReadings_Part1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đọc dữ liệu nhị phân
from json import JSONDecoder

try:
    json_text = raw[start:].decode("utf-8", errors="replace")
    outer_json = json.loads(json_text)
    inner_str = outer_json["data"]

    # Đọc phần JSON đầu tiên, bỏ qua phần dư
    decoder = JSONDecoder()
    inner_json, _ = decoder.raw_decode(inner_str)
    
    items = inner_json["items"]
except Exception as e:
    print(f"❌ Lỗi đọc JSON: {e}")
    exit()


# Chọn fields cần thiết
required_fields = ["@id", "dateTime", "measure", "value"]

def normalize_item(item):
    return {key: item.get(key, None) for key in required_fields}

rows = [Row(**normalize_item(item)) for item in items]
df = spark.createDataFrame(rows)

# Ghi ra silver layer
df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)
