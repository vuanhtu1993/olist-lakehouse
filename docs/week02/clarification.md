Container này tích hợp sẵn Apache Spark 3.5 (PySpark) và Jupyter Lab. Khi mở tệp notebook notebooks/eda.ipynb trên cổng localhost:8888, bạn đang sử dụng notebook như một "bảng điều khiển trung tâm" để làm việc với cả 2 môi trường tính toán:
```mermaid
flowchart TD
    subgraph UI ["1. Giao Diện Tương Tác (Interactive Canvas)"]
        NB["notebooks/eda_profiling.ipynb<br/>(Chạy trên container olist_spark_notebook :8888)"]
    end

    subgraph Engines ["2. Hai Môi Trường Tính Toán (Compute Engines)"]
        direction TB
        subgraph Engine1 ["Môi Trường 1: PostgreSQL Engine (OLTP SQL)"]
            PG_ENG["PostgreSQL 16 Engine<br/>• Thực thi câu lệnh SQL thuần (Raw SQL)<br/>• Khám phá 5 bảng transactional B0<br/>• Luyện tập 5 bài SQL hằng ngày"]
        end
        subgraph Engine2 ["Môi Trường 2: Apache Spark Engine (Distributed PySpark)"]
            SP_ENG["Apache Spark 3.5.0 Cluster<br/>• SparkSession với driver 1GB, multi-threading<br/>• Đọc PostgreSQL qua Spark JDBC<br/>• Đọc MinIO s3://landing/ qua S3A / S3FileIO<br/>• Phân tích phân tán tập dữ liệu lớn (1M dòng Geolocation)"]
        end
    end

    subgraph DataSources ["3. Hai Nguồn Dữ Liệu Lưu Trữ (Storage Layer)"]
        SRC_PG[("PostgreSQL Database<br/>(5 bảng transactional)")]
        SRC_S3[("MinIO s3://landing/<br/>(5 tệp CSV tham chiếu)")]
    end

    NB -->|1. Gửi câu truy vấn SQL qua SQLAlchemy / psycopg2 / Trino| PG_ENG
    NB -->|2. Gửi tác vụ PySpark DataFrame Transformation| SP_ENG

    PG_ENG --> SRC_PG
    SP_ENG -->|Spark JDBC| SRC_PG
    SP_ENG -->|S3 Protocol| SRC_S3

```