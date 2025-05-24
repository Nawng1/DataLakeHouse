from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("ToSilver_FloodWarnings") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "nang") \
    .config("spark.hadoop.fs.s3a.secret.key", "phuocnang123phuocnang123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đường dẫn
input_path = "s3a://data-lakehouse/bronze/flood-warnings/date_str=2025-05-22/"
output_path = "s3a://data-lakehouse/silver/flood-warnings/date=2025-05-22/"

# Định nghĩa schema cho JSON bên trong trường data
item_schema = StructType([
    StructField("@id", StringType(), True),
    StructField("description", StringType(), True),
    StructField("eaAreaName", StringType(), True),
    StructField("eaRegionName", StringType(), True),
    StructField("floodAreaID", StringType(), True),
    StructField("isTidal", BooleanType(), True),
    StructField("message", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("severityLevel", IntegerType(), True),
    StructField("timeMessageChanged", StringType(), True),
    StructField("timeRaised", StringType(), True),
    StructField("timeSeverityChanged", StringType(), True),
    StructField("floodArea", StructType([
        StructField("county", StringType(), True),
        StructField("notation", StringType(), True),
        StructField("polygon", StringType(), True),
        StructField("riverOrSea", StringType(), True)
    ]))
])

data_schema = StructType([
    StructField("items", ArrayType(item_schema), True)
])

# Đọc file dạng JSON raw (chứa field 'data' là chuỗi JSON string)
df_raw = spark.read.option("multiline", "true").json(input_path)

# Parse trường 'data' từ string -> struct
df_parsed = df_raw.withColumn("data_struct", from_json(col("data"), data_schema))

# explode và lấy item
df_exploded = df_parsed.select(explode("data_struct.items").alias("item")).select("item.*")

# Ghi ra silver
df_exploded.write.format("delta").mode("overwrite").save(output_path)
