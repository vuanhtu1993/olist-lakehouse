# Module infra_stack — Service & Interface Contracts

> **Parent Document:** [Module Spec](spec.md) | [Requirement Analysis Package](../../README.md)  
> **Module ID:** `INFRA` | **Scope:** Container Ports, Endpoints & Service Configs  
> **Linked Documents:** [Acceptance Criteria](acceptance-criteria.md)

---

## 1. Core Services Endpoint & Port Contract

Mọi container trong profile `core` phải tuân thủ nghiêm ngặt các cổng giao tiếp và biến môi trường sau:

| Service Name | Container Name | Host Port | Internal Port | Protocol | Purpose | Default Credentials (from `.env`) |
|---|---|---|---|---|---|---|
| `postgres` | `olist-postgres` | `5432` | `5432` | TCP / PG Wire | OLTP Source Database | User: `postgres`, DB: `oltp_olist` |
| `minio` | `olist-minio` | `9000` / `9001` | `9000` / `9001` | HTTP / S3 API | Object Storage & Web UI | AccessKey: `minioadmin`, Secret: `minioadmin` |
| `polaris` | `olist-polaris` | `8181` | `8181` | HTTP / REST | Iceberg REST Catalog | Bearer Auth / Client Credentials |
| `trino` | `olist-trino` | `8080` | `8080` | HTTP / Trino UI | Interactive SQL Engine | User: `admin` (No password on local) |
| `spark` | `olist-spark` | `8888` / `4040` | `8888` / `4040` | HTTP / Spark UI | PySpark Jupyter & UI | Token auth via `.env` |

---

## 2. Docker Volumes and Mount Points

Bảo đảm tính bền vững dữ liệu theo [`BR-M0-02`](../../global/business-rules.md#br-m0-02):

```yaml
volumes:
  postgres_data:
    driver: local
  minio_data:
    driver: local

services:
  postgres:
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./data_prep/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
  minio:
    volumes:
      - minio_data:/data
  trino:
    volumes:
      - ./infra/trino/etc:/etc/trino:ro
  spark:
    volumes:
      - ./spark_jobs:/home/jovyan/work/spark_jobs
      - ./notebooks:/home/jovyan/work/notebooks
```

---

## 3. Trino Iceberg Catalog Contract (`lakehouse.properties`)

Tệp tin cấu hình tại `infra/trino/etc/catalog/lakehouse.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog/v1
iceberg.rest-catalog.warehouse=lakehouse
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.ssl.enabled=false
```

---

## 4. Spark Iceberg Session Configuration Contract

Các biến cấu hình bắt buộc trong `SparkSession` (PySpark):

```python
conf = (
    SparkConf()
    .setAppName("Olist-Lakehouse")
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.lakehouse.type", "rest")
    .set("spark.sql.catalog.lakehouse.uri", "http://polaris:8181/api/catalog/v1")
    .set("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
    .set("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .set("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
    .set("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .set("spark.hadoop.fs.s3a.path.style.access", "true")
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
```

---
*Made by Anh Tu - Share to be share*
