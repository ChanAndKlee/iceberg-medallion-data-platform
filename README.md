# Modern Data Platform with Apache Iceberg

**Author:** Ariya Phengphon  
**Objective:** Design and implement a scalable, cost-efficient data platform supporting Apache Iceberg at terabyte scale.
**Table Format:** Apache Iceberg

---

## Table of Contents

- [1. Executive Summary](#1-executive-summary)
- [2. Architecture Overview](#2-architecture-overview)
  - [2.1 Why This Architecture](#21-why-this-architecture)
  - [2.2 Architecture Components](#22-architecture-components)
  - [2.3 Architecture Diagram](#23-architecture-diagram)
- [3. Data Flow Overview](#3-data-flow-overview)
- [4. ETL & Query data Demonstration](#4-etl--query-data-demonstration)
    - [4.1 Perform ETL using Spark](#41-perform-etl-using-spark)
    - [4.2 Connect to Trino & Query data](#42-connect-to-trino--query-data)
- [5. Cost Awareness Justification](#5-cost-awareness-justification)
- [6. Future Improvements](#6-future-improvements)
- [7. Setup & Installation](#7-setup--installation)

---

# 1. Executive Summary
This document proposes a scalable, cost-efficient modern data platform designed to:

- Support analytical workloads at **terabyte scale** while maintaining cost efficiency and operational simplicity.
- Handle **~100 concurrent analytical queries**
- Serve both **scheduled reporting** and **hourly monitoring workloads**
- Use **Apache Iceberg** as the primary table format
- Enable transactional consistency, schema evolution, and time travel
- Remain cost-aware and production extensible

The platform adopts a **Lakehouse architecture** built on open-source technologies, ensuring flexibility and scalability.

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

# 3. Data Flow Overview

1. Data is ingested from external sources via **Apache Spark**.
2. Raw data is stored in the **Bronze** namespace as Iceberg tables.
3. Spark transforms Bronze data into cleaned **Silver** tables.
4. Aggregated business-ready datasets are written to **Gold** tables.
5. **Trino** queries Iceberg tables for BI and analytics.
6. **BI tools** connect to Trino for reporting. In this project, user will use Dbeaver to query and analyze queries.

# 4. ETL & Query data Demonstration

## 4.1 Perform ETL using Spark
You can perform the ETL using Spark on each layer.
- Bronze: Pull from sources direcrly without transformation.
- Silver: Transform date to YYYY-MM-DD format, cast type, and rename column.
- Gold: Aggregate data based on business logic to be used in analytic and dashboard.

**Example:** Find the highest summation of total_profit across the sales date.

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

## 4.2 Connect to Trino & Query data
1. Download DBeaver program and add "Trino" as a datasource. Click test connection to check the connectivity before creating it.
![Connect Trino on DBeaver](/iceberg-platform/image/connect_trino_dbeaver.png)
2. Query data and result.
![Query data](/iceberg-platform/image/query_data_result.png)

---
# 5. Cost Awareness Justification

- **Cost-Effective Storage:** Object storage (MinIO/S3) is significantly cheaper than traditional data warehouse storage. It scales horizontally without requiring expensive provisioned storage.
- **Decoupled Compute Scaling:** Spark and Trino compute resources scale independently from storage. Compute nodes can be adjusted based on workload demand (e.g., heavy reporting windows), preventing over-provisioning.
- **No Data Duplication:** By using Apache Iceberg as a unified table format, Bronze, Silver, and Gold layers remain in the same storage system. This eliminates the need to copy data into a separate warehouse system, reducing storage and maintenance cost.
- **Open-Source Stack:** The platform relies entirely on open-source technologies (Iceberg, Spark, Trino, Nessie), avoiding licensing fees and vendor lock-in associated with proprietary data warehouse solutions.
- **Elastic Infrastructure:** In cloud deployment scenarios, object storage and distributed query engines support pay-as-you-go scaling, aligning infrastructure cost with actual usage.
- **Lightweight Local Deployment:** Docker Compose enables cost-efficient development and testing environments without requiring dedicated infrastructure.

---
# 6. Future Improvements
To evolve this platform toward a production-ready, enterprise-grade system, the following enhancements are recommended:
1. Expand Data Ingestion Capabilities
- Integrate additional batch sources (RDBMS, APIs, cloud storage).
- Introduce real-time streaming ingestion using Apache Kafka or equivalent event streaming platforms.
- Implement Change Data Capture (CDC) for incremental ingestion from operational databases.
2. CI/CD & Infrastructure Automation
- Introduce version-controlled deployment for safe environment promotion (dev -> staging -> prod).
3. Observability & Monitoring
- Use Grafana dashboards to monitor:
  - Query latency
  - ETL duration
  - Resource utilization (CPU/Mem/IO)
4. Alerting & Incident Management
- Integrate alerting systems to notify engineering teams via:
  - MSTeams
  - Slack
  - Email
5. Data Governance & Optimization
- Implement data quality checks using tools like Great Expectations.
- Define retention period policies and automated snapshot expiration on Iceberg to save the storage costs.
- Enable role-based access control (RBAC) for secure data access.

---
# 7. Setup & Installation
1. Clone git repository
```sh
git clone https://github.com/ChanAndKlee/iceberg-medallion-data-platform.git
```
2. Run docker
```sh
cd iceberg-platform
# Start docker container
docker compose up -d
docker ps

# After finish using, stop docker container
docker compose down
docker ps
```
![docker_ps](/iceberg-platform/image/docker_ps.png)

3. Pinned URL  
- Minio: http://localhost:9001/login (Required username, password: Please refer to file ```docker-compose.yml```)
- Nessie: http://localhost:19120/tree/main
- Trino: http://localhost:8080/ui/preview/ (Input with any username)