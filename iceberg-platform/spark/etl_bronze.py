from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.functions import col, to_timestamp, sum as _sum
from pyspark.sql.functions import date_format

spark = (
    SparkSession.builder
    .appName("iceberg-etl-bronze")
    # Iceberg + Spark runtime
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    # Nessie catalog
    .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
    .config("spark.sql.catalog.iceberg.ref", "main")
    # MinIO S3A
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Create schema (namespace)
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

bronze_df = (
    spark.read.option("header", True).csv("/opt/data/sales_data.csv")
    .withColumn("ingested_at", current_timestamp())
)
bronze_df.writeTo("iceberg.bronze.sales_raw").createOrReplace()
print("Bronze table created.")

spark.sql("SHOW TABLES IN iceberg.bronze").show()

spark.stop()
