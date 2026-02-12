# Modern Data Platform with Apache Iceberg

Written by Ariya Phengphon  
A scalable Lakehouse architecture using Apache Iceberg, Spark, Trino, and Nessie.

---

## Table of Contents

- [1. Problem Statement](#1-problem-statement)
- [2. Architecture Overview](#2-architecture-overview)
  - [2.1 Why This Architecture](#21-why-this-architecture)
  - [2.2 Architecture Components](#22-architecture-components)
  - [2.3 Architecture Diagram](#23-architecture-diagram)
- [4. Data Flow Overview](#4-data-flow-overview)
- [5. ETL & Query data Demonstration](#5-etl--query-data-demonstration)
    - [5.1 Perform ETL using Spark](#51-perform-etl-using-spark)
    - [5.2 Connect to Trino & Query data](#52-connect-to-trino--query-data)
- [6. Cost Awareness Justification](#6-cost-awareness-justification)
- [7. Future Improvements](#7-future-improvements)
- [8. Setup & Installation](#8-setup--installation)
<!-- - [9. Versioning & Governance](#9-versioning--governance)
- [10. Failure & Recovery Strategy](#10-failure--recovery-strategy) -->
<!-- - [11. Future Improvements](#11-future-improvements) -->

---

# 1. Problem Statement
Design and implement a scalable data platform that:

- Uses **Apache Iceberg** as the main table format
- Supports **terabyte-scale datasets**
- Handles multiple query patterns:
  - Scheduled reporting
  - Hourly monitoring workloads
- Supports **~100 concurrent queries per reporting request**
- Demonstrates one ETL pipeline storing data as Iceberg tables
- Allows users to connect and query the platform
- Maintains cost awareness

---

# 2. Architecture Overview

## 2.1 Why This Architecture?

This architecture ensures scalability, consistency, and cost efficiency.

- **Decoupled Compute & Storage:** Spark and Trino operate independently from object storage (MinIO), enabling horizontal scaling.
- **Unified Lakehouse Design:** All Bronze, Silver, and Gold data (Medallion Architecture) remain in object storage using Apache Iceberg, eliminating data duplication and data silos.
- **Transactional Consistency:** Iceberg provides ACID guarantees and snapshot isolation for concurrent workloads.
- **Versioned Metadata:** Nessie enables Git-like metadata versioning and safe experimentation.
- **High Concurrency Support:** Trino’s distributed architecture handles analytical workloads with ~100 concurrent queries.
- **Cost Efficient:** Open-source components and object storage reduce infrastructure and licensing costs.


## 2.2 Architecture Components

| Layer | Component | Responsibility |
|-------|-----------|----------------|
| **1. Processing Layer** | Apache Spark | Handles data ingestion, transformation, and ETL workloads across Bronze, Silver, and Gold layers. |
| **2. Metadata Layer** | Project Nessie | Versioned catalog managing Iceberg table references and enabling metadata branching. |
|  | PostgreSQL | Backend database persisting Nessie metadata and commit history. |
| **3. Storage Layer** | MinIO (S3-compatible) | Stores Iceberg data files and metadata files. Scalable object storage storing Iceberg data files (Parquet) and metadata files. |
| **4. Query Engine Layer** | Trino | Distributed SQL engine enabling high-concurrency analytics and BI workloads. |
| **5. Consumption Layer** | BI Tools (Tableau, DBeaver, Superset) | Provides reporting, dashboarding, and ad-hoc analytics via Trino. |
|  | Trino CLI / JDBC | Enables SQL-based access for developers and applications. |
| **6. Deployment Layer** | Docker Compose | Containerized orchestration for local and development environments. |

---

## 2.3 Architecture Diagram

![Architecture](/iceberg-platform/image/architecture-design.png)

---

# 4. Data Flow Overview

1. Data is ingested from external sources via **Apache Spark**.
2. Raw data is stored in the **Bronze** namespace as Iceberg tables.
3. Spark transforms Bronze data into cleaned **Silver** tables.
4. Aggregated business-ready datasets are written to **Gold** tables.
5. **Trino** queries Iceberg tables for BI and analytics.
6. **BI tools** connect to Trino for reporting. In this project, user will use Dbeaver to query and analyze queries.

# 5. ETL & Query data Demonstration

## 5.1 Perform ETL using Spark
You can perform the ETL using Spark on each layer.
- Bronze: Pull from sources direcrly without transformation.
- Silver: Transform date to YYYY-MM-DD format, cast type, rename column.
- Gold: Aggregate data based on business logic to be used in analytic and dashboard.

Example: Find the highest total_profit summation.

1. Creating Gold Aggregation Table (Refer to this file: ```iceberg-platform/spark/etl_gold.py```)

```py
from pyspark.sql.functions import sum as _sum, count

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

# Read data from silver layer table
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

# Write to gold layer table
gold_df.writeTo("iceberg.gold.sales_daily_summary").createOrReplace()
```

2. Run this file by using spark submit.
```sh
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/etl_gold.py
```
![ETL Output](/iceberg-platform/image/etl_output.png)

## 5.2 Connect to Trino & Query data
1. Download DBeaver program and add "Trino" as data source. Click test connection to check the connectivity.
![Connect Trino on DBeaver](/iceberg-platform/image/connect_trino_dbeaver.png)
2. Query data and result.
![Query data](/iceberg-platform/image/query_data_result.png)

---
# 6. Cost Awareness Justification

- **Cost-Effective Storage:** Object storage (MinIO/S3) is significantly cheaper than traditional data warehouse storage. It scales horizontally without requiring expensive provisioned storage.
- **Decoupled Compute Scaling:** Spark and Trino compute resources scale independently from storage. Compute nodes can be adjusted based on workload demand (e.g., heavy reporting windows), preventing over-provisioning.
- **No Data Duplication:** By using Apache Iceberg as a unified table format, Bronze, Silver, and Gold layers remain in the same storage system. This eliminates the need to copy data into a separate warehouse system, reducing storage and maintenance cost.
- **Open-Source Stack:** The platform relies entirely on open-source technologies (Iceberg, Spark, Trino, Nessie), avoiding licensing fees and vendor lock-in associated with proprietary data warehouse solutions.
- **Elastic Infrastructure:** In cloud deployment scenarios, object storage and distributed query engines support pay-as-you-go scaling, aligning infrastructure cost with actual usage.
- **Lightweight Local Deployment:** Docker Compose enables cost-efficient development and testing environments without requiring dedicated infrastructure.

---
# 7. Future Improvements
1. Add more data sources. For instance, database or streaming ingestion like Kafka.
2. Add CI/CD pipeline
3. Introduce monitoring (Prometheus + Grafana)
4. Send alert that collected from monitoring to team channel (ex. MSTeams)

---
# 8. Setup & Installation
1. Clone git repository
```sh
git clone https://github.com/ChanAndKlee/iceberg-medallion-data-platform.git
```
2. Run docker
```sh
cd iceverg-platform
# Start docker container
docker compose up -d
docker ps

# After finish using, stop docker container
docker compose down
docker ps
```
3. Pinned URL  
- Minio: http://localhost:9001/login (Required username, password: Please refer to file ```docker-compose.yml```)
- Nessie: http://localhost:19120/tree/main
- Trino: http://localhost:8080/ui/preview/ (Input with any username)