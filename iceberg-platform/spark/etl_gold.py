from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg

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
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

silver_df = spark.read.table("iceberg.silver.sales_cleaned")

gold_df = (
    silver_df
    .groupBy("sale_date", "region")
    .agg(
        _sum("calculated_revenue").alias("total_revenue"),
        _sum("profit").alias("total_profit"),
        _sum("quantity_sold").alias("total_quantity"),
        count("*").alias("total_transactions")
    )
)

gold_df.writeTo("iceberg.gold.sales_daily_summary").createOrReplace()
print("Bronze table created.")

spark.sql("SHOW TABLES IN iceberg.gold").show()

spark.stop()