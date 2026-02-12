from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lower, trim, current_timestamp

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
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

bronze_df = spark.read.table("iceberg.bronze.sales_raw")

silver_df = (
    bronze_df
    # Rename columns to snake_case
    .withColumnRenamed("Product_ID", "product_id")
    .withColumnRenamed("Sale_Date", "sale_date")
    .withColumnRenamed("Sales_Rep", "sales_rep")
    .withColumnRenamed("Region", "region")
    .withColumnRenamed("Sales_Amount", "sales_amount")
    .withColumnRenamed("Quantity_Sold", "quantity_sold")
    .withColumnRenamed("Product_Category", "product_category")
    .withColumnRenamed("Unit_Cost", "unit_cost")
    .withColumnRenamed("Unit_Price", "unit_price")
    .withColumnRenamed("Customer_Type", "customer_type")
    .withColumnRenamed("Discount", "discount")
    .withColumnRenamed("Payment_Method", "payment_method")
    .withColumnRenamed("Sales_Channel", "sales_channel")

    # Fix types
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("sales_amount", col("sales_amount").cast("double"))
    .withColumn("quantity_sold", col("quantity_sold").cast("int"))
    .withColumn("unit_cost", col("unit_cost").cast("double"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("discount", col("discount").cast("double"))

    # Clean strings
    .withColumn("sales_rep", trim(lower(col("sales_rep"))))
    .withColumn("region", trim(lower(col("region"))))
    .withColumn("product_category", trim(lower(col("product_category"))))
    .withColumn("customer_type", trim(lower(col("customer_type"))))
    .withColumn("payment_method", trim(lower(col("payment_method"))))
    .withColumn("sales_channel", trim(lower(col("sales_channel"))))

    # Recalculate revenue (don’t fully trust raw Sales_Amount)
    .withColumn(
        "calculated_revenue",
        col("unit_price") * col("quantity_sold") * (1 - col("discount"))
    )

    # Calculate profit
    .withColumn(
        "profit",
        (col("unit_price") - col("unit_cost")) *
        col("quantity_sold") *
        (1 - col("discount"))
    )

    # Remove useless derived column
    .drop("Region_and_Sales_Rep")

    # Add metadata
    .withColumn("processed_at", current_timestamp())
)

silver_df.writeTo("iceberg.silver.sales_cleaned").createOrReplace()
print("Bronze table created.")

spark.sql("SHOW TABLES IN iceberg.silver").show()

spark.stop()