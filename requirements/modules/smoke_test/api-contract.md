# Module smoke_test — Pipeline & Verification Contracts

> **Parent Document:** [Module Spec](spec.md) | [Requirement Analysis Package](../../README.md)  
> **Module ID:** `SMOKE` | **Scope:** Data Access APIs & End-to-End Test Interfaces  
> **Linked Documents:** [Acceptance Criteria](acceptance-criteria.md)

---

## 1. JDBC Source Connection Contract

Spark thực hiện trích xuất dữ liệu qua JDBC driver `org.postgresql.Driver`:

```python
jdbc_url = "jdbc:postgresql://postgres:5432/oltp_olist"
connection_properties = {
    "user": "postgres",
    "password": "POSTGRES_PASSWORD_FROM_ENV",
    "driver": "org.postgresql.Driver"
}

df_orders = spark.read.jdbc(
    url=jdbc_url,
    table="orders",
    properties=connection_properties
)
```

---

## 2. Iceberg Table Write Contract

Tạo namespace và ghi bảng Iceberg tầng Bronze trên MinIO:

```sql
-- DDL tạo namespace và bảng Iceberg
CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.oms_orders_smoke (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    updated_at TIMESTAMP,
    _ingest_timestamp TIMESTAMP
)
USING iceberg
LOCATION 's3a://bronze/oms_orders_smoke'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
```

```python
# Spark Data Write Contract
(
    df_orders
    .withColumn("_ingest_timestamp", current_timestamp())
    .writeTo("lakehouse.bronze.oms_orders_smoke")
    .append()
)
```

---

## 3. Trino Query and Tie-out Contract

Truy vấn đối soát từ Trino CLI / Python client:

```sql
-- Query kiểm tra sự hiện diện của bảng
SHOW TABLES IN lakehouse.bronze;

-- Query đối soát số lượng bản ghi (Tie-out)
SELECT count(*) AS total_orders FROM lakehouse.bronze.oms_orders_smoke;

-- Query kiểm tra Time Travel & Metadata Snapshots
SELECT snapshot_id, committed_at, operation, summary 
FROM lakehouse.bronze."oms_orders_smoke$snapshots";
```

---

## 4. MinIO Storage Audit Contract

Cấu trúc tệp tin bắt buộc phải xuất hiện trong MinIO bucket `bronze/oms_orders_smoke`:

```
bronze/
└── oms_orders_smoke/
    ├── metadata/
    │   ├── v1.metadata.json           <-- Con trỏ bảng Iceberg
    │   ├── snap-*.avro                <-- Manifest List
    │   └── *.avro                     <-- Manifest File
    └── data/
        └── *.zstd.parquet             <-- File dữ liệu cột thực tế
```

---
*Made by Anh Tu - Share to be share*
