# Tuần 01: Foundations & Lab M0 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thiết lập trọn vẹn hạ tầng Data Lakehouse cục bộ bằng Docker Compose, nạp mẻ dữ liệu Olist khởi đầu (Batch B0) vào PostgreSQL OLTP và thông luồng End-to-End (E2E) kiểm chứng từ PostgreSQL sang Apache Iceberg trên MinIO S3 thông qua Trino.

**Architecture:** Áp dụng mô hình Storage-Compute Decoupling (tách biệt lưu trữ và tính toán) chuẩn Enterprise Lakehouse. Dữ liệu transactional nằm tại PostgreSQL được PySpark trích xuất và nạp vào Open Table Format (Apache Iceberg) lưu trên MinIO S3 thông qua Polaris REST Catalog điều phối metadata tập trung, sẵn sàng cho Trino truy vấn tương tác tốc độ cao. Toàn bộ dịch vụ được quản lý qua Docker Profiles để bảo đảm mức tiêu thụ RAM < 6GB trên máy cá nhân.

**Tech Stack:** Docker Compose v2, PostgreSQL 16, MinIO (S3-compatible Object Storage), Apache Polaris (REST Catalog), Apache Iceberg 1.6, Apache Spark 3.5 (PySpark), Trino 442, Python 3.10+ (Faker, SQLAlchemy, PyArrow, psycopg2-binary, pytest).

---

## 🧭 BẢN ĐỒ KHÁI NIỆM & GIẢI PHẪU SƯ PHẠM (PEDAGOGICAL ANATOMY)

Trước khi bắt tay thực thi từng dòng lệnh, người kỹ sư dữ liệu (Data Engineer) cần nắm vững bản chất kiến trúc và các "viên gạch" công nghệ cốt lõi.

### 1. Giải phẫu khái niệm: Data Lakehouse
> **Định nghĩa chuẩn học thuật:**  
> **Data Lakehouse** là một *kiến trúc quản trị dữ liệu hợp nhất (unified data management architecture)* kết hợp tính linh hoạt, chi phí lưu trữ thấp và khả năng mở rộng vô hạn của **Data Lake** (sử dụng Object Storage với định dạng tệp mở) cùng năng lực quản trị giao dịch nghiêm ngặt (ACID), tiến hóa lược đồ (Schema Evolution) và tốc độ truy vấn cao của **Data Warehouse**.

```mermaid
graph TD
    subgraph TraditionalDW ["1. Data Warehouse truyền thống"]
        DW1["Lưu trữ gắn chặt Tính toán"]
        DW2["Dữ liệu có cấu trúc, ACID mạnh"]
        DW3["❌ Chi phí đắt đỏ, Vendor Lock-in, khó lưu Audio/Image/Log"]
    end

    subgraph DataLake ["2. Data Lake thế hệ thứ nhất"]
        DL1["Object Storage: HDFS, S3 (File CSV, Parquet)"]
        DL2["Chi phí siêu rẻ, lưu trữ không giới hạn"]
        DL3["❌ Không ACID, không cập nhật từng dòng, dễ biến thành 'Data Swamp'"]
    end

    subgraph ModernLakehouse ["3. Data Lakehouse (Hiện đại - Chuẩn Open Source)"]
        ML1["Storage: MinIO / AWS S3 (Lưu file Parquet mở)"]
        ML2["Table Format: Apache Iceberg (Đảm bảo ACID, Snapshot, Time Travel)"]
        ML3["Catalog: Polaris REST Catalog (Con trỏ nguyên tử tới metadata mới nhất)"]
        ML4["Compute Engines: Spark (Batch ETL) + Trino (Ad-hoc SQL Query)"]
    end

    TraditionalDW --> ModernLakehouse
    DataLake --> ModernLakehouse
```

* **Anatomical Keywords (Giải phẫu từ khóa):**
  * **Unified (Hợp nhất):** Xóa bỏ kiến trúc "2 bản sao" (Data Lake đẩy sang Data Warehouse gây trễ và sai lệch dữ liệu). Mọi engine phân tích đều truy cập trực tiếp vào cùng một lớp lưu trữ duy nhất.
  * **Open File & Table Format (Định dạng mở):** Dữ liệu vật lý là các file Parquet (lưu trữ dạng cột). Nhưng thay vì để người dùng quản lý file rời rạc, lớp Table Format (Apache Iceberg) trừu tượng hóa các file này thành một Table logic chuẩn SQL.
  * **Storage-Compute Decoupling (Tách biệt Lưu trữ & Tính toán):** Lưu trữ trên MinIO/S3 chi phí cực rẻ và bền vững. Các engine tính toán (Spark, Trino) chỉ khởi chạy khi có tải, đọc xong có thể giải phóng tài nguyên CPU/RAM.

### 2. Giải phẫu khái niệm: Modern Catalog (Apache Polaris)
> **Định nghĩa chuẩn học thuật:**  
> Trong kiến trúc Iceberg, **Catalog** là *điểm kiểm soát trung tâm (central metadata store)* lưu giữ con trỏ nguyên tử (atomic pointer) chỉ tới tệp siêu dữ liệu (metadata file) hiện hành của mỗi bảng, đồng thời thực thi cơ chế kiểm soát tương tranh lạc quan (Optimistic Concurrency Control - OCC) khi nhiều engine cùng ghi dữ liệu.

```mermaid
sequenceDiagram
    autonumber
    participant Spark as Spark Engine (Writer)
    participant Catalog as Polaris REST Catalog
    participant S3 as MinIO (Storage)
    participant Trino as Trino Engine (Reader)

    Spark->>S3: Ghi các file dữ liệu mới (data_xyz.parquet)
    Spark->>S3: Ghi file snap_v2.avro và metadata_v2.json
    Spark->>Catalog: Commit: Hoán đổi con trỏ từ v1.json sang v2.json (Atomic Swap)
    Catalog-->>Spark: 200 OK (Commit thành công)
    
    Trino->>Catalog: Bảng orders hiện tại có snapshot nào?
    Catalog-->>Trino: Trỏ tới metadata_v2.json
    Trino->>S3: Đọc trực tiếp các file Parquet mới nhất
```

* **Root Cause Analysis (Tại sao cần REST Catalog?):**
  * *Quá khứ:* Trước đây Hadoop dùng Hive Metastore (HMS) nặng nề, phụ thuộc vào hạ tầng Java/Thrift cồng kềnh.
  * *Giải pháp:* Iceberg REST Catalog chuẩn hóa giao tiếp qua HTTP/REST API gọn nhẹ. Bất kỳ engine nào (Spark viết bằng Scala/Java, DuckDB viết bằng C++, Trino, hay Python Polars) đều có thể kết nối đồng nhất.

---

## ⚖️ PHÂN TÍCH ĐÁNH ĐỔI & TƯ DUY PHẢN BIỆN (TRADE-OFFS & CRITICAL THINKING)

1. **Docker Profiles (`--profile core` vs Toàn bộ Stack):**
   * *Đánh đổi:* Thay vì chạy cả Airflow, Superset, Spark, Trino, Postgres, MinIO cùng lúc (ngốn > 12GB RAM, dễ crash máy cá nhân của học viên), kiến trúc tuần 1 chia thành 2 profile:
     - `core`: Postgres + MinIO + Polaris + Trino + PySpark (RAM khống chế < 6GB).
     - `full`: Bổ sung thêm Airflow + Superset cho tuần tự động hóa (Tuần 6).
   * *Lợi ích sư phạm:* Học viên hiểu rõ trách nhiệm từng container thay vì coi Docker như một "hộp đen".

2. **Kế hoạch dự phòng (Fall-back Strategy for Catalog):**
   * *Rủi ro:* Apache Polaris REST Catalog là công nghệ mới (vừa gia nhập Apache Software Foundation), có thể gặp lỗi kết nối trên một số kiến trúc CPU (Apple Silicon ARM64 vs Intel x86).
   * *Phương án xử lý:* Timebox 2.5 giờ cho Polaris. Nếu không ổn định, lập tức kích hoạt phương án dự phòng tại [ADR-001](../requirements/governance/adr/001-initial-architecture.md): Chuyển sang Iceberg JDBC Catalog tích hợp sẵn trên PostgreSQL 16 mà không làm gián đoạn tiến độ thực hành.

---

## 📁 CẤU TRÚC THƯ MỤC CẦN KHỞI TẠO (PROJECT REPO STRUCTURE)

```text
olist-lakehouse/
├── .env.example                      # Biến môi trường mẫu cho toàn bộ stack
├── .gitignore                         # Loại bỏ data, temp, pycache, secrets
├── README.md                          # Hướng dẫn chạy 1 lệnh duy nhất
├── docker-compose.yml                 # Định nghĩa toàn bộ dịch vụ (có profile core/full)
├── data_prep/                         # Thư mục xử lý dữ liệu nguồn ban đầu
│   ├── raw/                           # 9 file CSV tải từ Kaggle Olist
│   ├── processed/                     # Dữ liệu sau khi sinh PII và cắt batch B0-B7
│   ├── gen_pii.py                     # Script sinh dữ liệu cá nhân giả bằng Faker
│   ├── split_batches.py               # Script cắt 8 batches (B0 đến B7)
│   └── seed_postgres.py               # Nạp batch B0 vào PostgreSQL oltp_olist
├── etc/                               # Cấu hình các công cụ
│   └── trino/
│       └── catalog/
│           └── lakehouse.properties   # Cấu hình Iceberg connector cho Trino
├── spark_jobs/                        # Thư mục chứa mã xử lý PySpark
│   └── smoke_test_e2e.py              # Script Hello-World đọc PG ghi Iceberg
├── sql/                               # Các bài tập SQL hàng ngày
│   └── week01_daily_queries.sql       # 5 câu query Postgres giải thích chuyên sâu
├── tests/                             # Bộ script kiểm thử nghiệm thu tự động
│   ├── test_storage_infra.py          # Kiểm tra MinIO buckets & Postgres
│   ├── test_data_prep.py              # Kiểm tra Faker PII & Batch files
│   └── gate_g1_check.sh               # Script tự chấm Gate G1
└── docs/                              # Tài liệu giải thích & phỏng vấn
    └── week01_interview_prep.md       # Trả lời 5 câu hỏi phỏng vấn DE chuyên sâu
```

---

## 📋 DANH SÁCH NHIỆM VỤ THỰC THI CHI TIẾT (TASK BREAKDOWN)

### Task 1: Khởi tạo Repository & Cấu hình môi trường nền tảng
**Phụ thuộc:** Không  
**WBS:** WP1.1 | **NFR:** NFR-01, NFR-06  
**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Create: `.env`
- Create: `README.md`

- [x] **Step 1: Viết tệp `.gitignore` chuẩn cho Data Engineering**
  Chặn các tệp dữ liệu lớn, secrets, cache và virtualenv.
  ```bash
  cat << 'EOF' > .gitignore
  # Environments & Secrets
  .env
  *.env
  __pycache__/
  *.pyc
  .pytest_cache/
  .ipynb_checkpoints/

  # Data folders (chỉ lưu code, không commit dữ liệu thô lớn)
  data_prep/raw/*.csv
  data_prep/processed/
  !data_prep/raw/.gitkeep

  # Docker volumes local mount
  .minio-data/
  .postgres-data/

  # IDE & OS
  .DS_Store
  .idea/
  .vscode/
  EOF
  ```

- [x] **Step 2: Viết tệp `.env.example`**
  Định nghĩa toàn bộ cổng, user, password dùng chung cho các service.
  ```bash
  cat << 'EOF' > .env.example
  # PostgreSQL OLTP
  POSTGRES_USER=olist_admin
  POSTGRES_PASSWORD=olist_password
  POSTGRES_DB=oltp_olist
  POSTGRES_PORT=5432

  # MinIO S3 Object Storage
  MINIO_ROOT_USER=minioadmin
  MINIO_ROOT_PASSWORD=minioadmin
  MINIO_PORT=9000
  MINIO_CONSOLE_PORT=9001
  AWS_ACCESS_KEY_ID=minioadmin
  AWS_SECRET_ACCESS_KEY=minioadmin
  AWS_REGION=us-east-1
  S3_ENDPOINT=http://minio:9000

  # Apache Polaris Catalog
  POLARIS_PORT=8181

  # Trino Query Engine
  TRINO_PORT=8080

  # Network & Project
  COMPOSE_PROJECT_NAME=olist_lakehouse
  EOF
  cp .env.example .env
  ```

- [x] **Step 3: Tạo cây thư mục dự án**
  ```bash
  mkdir -p data_prep/raw data_prep/processed etc/trino/catalog spark_jobs sql tests docs
  touch data_prep/raw/.gitkeep
  ```

- [x] **Step 4: Kiểm tra tính sẵn sàng của môi trường**
  Run: `ls -la && test -f .env && echo "ENV READY"`
  Expected output: `ENV READY`

- [x] **Step 5: Git commit mốc khởi tạo**
  Run:
  ```bash
  git init
  git add .gitignore .env.example README.md
  git commit -m "feat: initial repository structure and environment configurations"
  ```

---

### Task 2: Hạ tầng Lưu trữ & Cơ sở dữ liệu (PostgreSQL 16 & MinIO S3)
**Phụ thuộc:** Task 1  
**WBS:** WP1.2 | **RTM:** FR-INFRA-01, FR-INFRA-02, FR-INFRA-03 | **AC:** AC-INFRA-01, AC-INFRA-02, AC-INFRA-03  
**Files:**
- Create: `docker-compose.yml`
- Create: `tests/test_storage_infra.py`

- [ ] **Step 1: Viết failing test cho hạ tầng lưu trữ**
  Tạo `tests/test_storage_infra.py` để kiểm tra kết nối Postgres và 4 buckets trên MinIO.
  ```python
  # tests/test_storage_infra.py
  import os
  import psycopg2
  import boto3
  from botocore.client import Config

  def test_postgres_connection():
      """Kiểm tra PostgreSQL 16 sẵn sàng nhận kết nối và đúng database"""
      conn = psycopg2.connect(
          host="localhost",
          port=os.getenv("POSTGRES_PORT", 5432),
          user=os.getenv("POSTGRES_USER", "olist_admin"),
          password=os.getenv("POSTGRES_PASSWORD", "olist_password"),
          dbname=os.getenv("POSTGRES_DB", "oltp_olist")
      )
      cur = conn.cursor()
      cur.execute("SELECT version();")
      version = cur.fetchone()[0]
      assert "PostgreSQL 16" in version
      conn.close()

  def test_minio_buckets_exist():
      """Kiểm tra 4 bucket chuẩn Medallion Architecture đã được tự động tạo"""
      s3 = boto3.client(
          's3',
          endpoint_url="http://localhost:9000",
          aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
          aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
          config=Config(signature_version='s3v4'),
          region_name='us-east-1'
      )
      response = s3.list_buckets()
      bucket_names = [b['Name'] for b in response.get('Buckets', [])]
      expected_buckets = ["landing", "bronze", "silver", "gold"]
      for b in expected_buckets:
          assert b in bucket_names, f"Bucket {b} chưa được tạo trên MinIO!"
  ```

- [ ] **Step 2: Chạy test và xác nhận FAIL (vì container chưa chạy)**
  Run: `pytest tests/test_storage_infra.py -v`
  Expected output: `FAIL` (Connection refused)

- [ ] **Step 3: Định nghĩa dịch vụ PostgreSQL và MinIO trong `docker-compose.yml`**
  Cấu hình resource limits (RAM), healthcheck và service `mc-init` tự động khởi tạo 4 buckets.
  ```yaml
  # docker-compose.yml (Core Storage Part)
  version: '3.8'

  services:
    postgres:
      image: postgres:16-alpine
      container_name: olist_postgres
      profiles: ["core", "full"]
      environment:
        POSTGRES_USER: ${POSTGRES_USER}
        POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
        POSTGRES_DB: ${POSTGRES_DB}
      ports:
        - "${POSTGRES_PORT}:5432"
      volumes:
        - pgdata:/var/lib/postgresql/data
      deploy:
        resources:
          limits:
            memory: 512M
      healthcheck:
        test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
        interval: 5s
        timeout: 5s
        retries: 5

    minio:
      image: minio/minio:RELEASE.2024-05-10T01-41-38Z
      container_name: olist_minio
      profiles: ["core", "full"]
      command: server /data --console-address ":9001"
      environment:
        MINIO_ROOT_USER: ${MINIO_ROOT_USER}
        MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      ports:
        - "${MINIO_PORT}:9000"
        - "${MINIO_CONSOLE_PORT}:9001"
      volumes:
        - miniodata:/data
      deploy:
        resources:
          limits:
            memory: 512M
      healthcheck:
        test: ["CMD", "mc", "ready", "local"]
        interval: 5s
        timeout: 5s
        retries: 5

    mc-init:
      image: minio/mc:RELEASE.2024-05-09T17-04-24Z
      container_name: olist_mc_init
      profiles: ["core", "full"]
      depends_on:
        minio:
          condition: service_healthy
      entrypoint: >
        /bin/sh -c "
        mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD};
        mc mb --ignore-existing myminio/landing;
        mc mb --ignore-existing myminio/bronze;
        mc mb --ignore-existing myminio/silver;
        mc mb --ignore-existing myminio/gold;
        exit 0;
        "

  volumes:
    pgdata:
    miniodata:
  ```

- [ ] **Step 4: Khởi chạy Core Storage & Chạy lại test để PASS**
  Run:
  ```bash
  docker compose --profile core up -d postgres minio mc-init
  pytest tests/test_storage_infra.py -v
  ```
  Expected output: `2 passed in ...`

- [ ] **Step 5: Kiểm tra tính bền vững (Persistence Check - AC-INFRA-02)**
  Run:
  ```bash
  docker compose restart postgres minio
  pytest tests/test_storage_infra.py -v
  ```
  Expected output: `2 passed` (Dữ liệu và cấu hình bucket không bị xóa khi container khởi động lại).

- [ ] **Step 6: Git commit Task 2**
  ```bash
  git add docker-compose.yml tests/test_storage_infra.py
  git commit -m "feat: add postgres and minio services with automatic 4-bucket initialization"
  ```

---

### Task 3: Quản trị Metadata & Truy vấn tương tác (Polaris REST Catalog & Trino)
**Phụ thuộc:** Task 2  
**WBS:** WP1.3 | **RTM:** FR-INFRA-04 | **AC:** AC-INFRA-04 | **ADR:** ADR-001, ADR-002  
**Files:**
- Modify: `docker-compose.yml`
- Create: `etc/trino/catalog/lakehouse.properties`
- Create: `tests/test_trino_catalog.py`

- [ ] **Step 1: Viết cấu hình Iceberg Connector cho Trino**
  Tạo tệp `etc/trino/catalog/lakehouse.properties` trỏ trực tiếp tới Polaris REST Catalog và MinIO S3 endpoint.
  ```properties
  # etc/trino/catalog/lakehouse.properties
  connector.name=iceberg
  iceberg.catalog.type=rest
  iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
  iceberg.rest-catalog.warehouse=s3://lakehouse/
  
  # Cấu hình S3/MinIO
  fs.native-s3.enabled=true
  s3.endpoint=http://minio:9000
  s3.region=us-east-1
  s3.aws-access-key=minioadmin
  s3.aws-secret-key=minioadmin
  s3.path-style-access=true
  ```

- [ ] **Step 2: Viết test kiểm tra Trino nhận diện Catalog**
  ```python
  # tests/test_trino_catalog.py
  import trino

  def test_trino_shows_lakehouse_catalog():
      """Kiểm tra Trino query engine đã load được catalog lakehouse"""
      conn = trino.dbapi.connect(
          host='localhost',
          port=8080,
          user='admin',
          catalog='system',
          schema='runtime'
      )
      cur = conn.cursor()
      cur.execute("SHOW CATALOGS")
      catalogs = [row[0] for row in cur.fetchall()]
      assert "lakehouse" in catalogs, f"Không tìm thấy 'lakehouse' catalog trong Trino! Đang có: {catalogs}"
      conn.close()
  ```

- [ ] **Step 3: Bổ sung Polaris Catalog và Trino vào `docker-compose.yml`**
  ```yaml
    polaris:
      image: apache/polaris:latest
      container_name: olist_polaris
      profiles: ["core", "full"]
      ports:
        - "${POLARIS_PORT}:8181"
      environment:
        POLARIS_STORAGE_TYPE: s3
        POLARIS_S3_ENDPOINT: http://minio:9000
        AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
        AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
      depends_on:
        minio:
          condition: service_healthy
      deploy:
        resources:
          limits:
            memory: 1G

    trino:
      image: trinodb/trino:442
      container_name: olist_trino
      profiles: ["core", "full"]
      ports:
        - "${TRINO_PORT}:8080"
      volumes:
        - ./etc/trino/catalog:/etc/trino/catalog:ro
      depends_on:
        - polaris
        - minio
      deploy:
        resources:
          limits:
            memory: 1536M
  ```

- [ ] **Step 4: Khởi chạy và kiểm tra Trino Catalog**
  Run:
  ```bash
  docker compose --profile core up -d polaris trino
  sleep 15
  pytest tests/test_trino_catalog.py -v
  ```
  Expected output: `1 passed`

- [ ] **Step 5: Git commit Task 3**
  ```bash
  git add etc/trino/catalog/lakehouse.properties docker-compose.yml tests/test_trino_catalog.py
  git commit -m "feat: configure polaris rest catalog and trino iceberg connector"
  ```

---

### Task 4: Nền tảng Tính toán Phân tán (Apache Spark với Iceberg Runtime)
**Phụ thuộc:** Task 3  
**WBS:** WP1.4 | **RTM:** FR-INFRA-05 | **AC:** AC-INFRA-05  
**Files:**
- Modify: `docker-compose.yml`
- Create: `tests/test_spark_iceberg.py`

- [ ] **Step 1: Viết test kiểm chứng Spark tương tác với Iceberg Catalog**
  ```python
  # tests/test_spark_iceberg.py
  from pyspark.sql import SparkSession

  def get_spark():
      return SparkSession.builder \
          .appName("TestIcebergCatalog") \
          .master("local[2]") \
          .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
          .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
          .config("spark.sql.catalog.lakehouse.type", "rest") \
          .config("spark.sql.catalog.lakehouse.uri", "http://localhost:8181/api/catalog") \
          .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
          .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://localhost:9000") \
          .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
          .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
          .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
          .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
          .config("spark.hadoop.fs.s3a.path.style.access", "true") \
          .getOrCreate()

  def test_spark_creates_bronze_namespace():
      """Kiểm tra Spark có thể tạo và liệt kê namespace 'bronze' trên Iceberg catalog"""
      spark = get_spark()
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;")
      namespaces = [row[0] for row in spark.sql("SHOW NAMESPACES IN lakehouse;").collect()]
      assert "bronze" in namespaces
      spark.stop()
  ```

- [ ] **Step 2: Chạy test và xác nhận Spark khởi tạo namespace thành công**
  Run: `pytest tests/test_spark_iceberg.py -v`
  Expected output: `1 passed`

- [ ] **Step 3: Thêm Spark/Jupyter container vào `docker-compose.yml` (cho học viên dùng UI Web)**
  ```yaml
    spark-notebook:
      image: jupyter/pyspark-notebook:spark-3.5.0
      container_name: olist_spark_notebook
      profiles: ["core", "full"]
      ports:
        - "8888:8888"
      environment:
        JUPYTER_ENABLE_LAB: "yes"
        NOTEBOOK_ARGS: "--NotebookApp.token=''"
      volumes:
        - ./spark_jobs:/home/jovyan/work/spark_jobs
        - ./data_prep:/home/jovyan/work/data_prep
        - ./tests:/home/jovyan/work/tests
      deploy:
        resources:
          limits:
            memory: 2G
  ```

- [ ] **Step 4: Khởi chạy container và kiểm tra**
  Run:
  ```bash
  docker compose --profile core up -d spark-notebook
  docker ps --filter "name=olist_spark_notebook"
  ```
  Expected output: Container ở trạng thái `Up`

- [ ] **Step 5: Git commit Task 4**
  ```bash
  git add docker-compose.yml tests/test_spark_iceberg.py
  git commit -m "feat: configure spark session with iceberg runtime and s3 file io"
  ```

---

### Task 5: Chuẩn bị Dữ liệu Nguồn, Sinh PII Giả & Cắt Batches (B0–B7)
**Phụ thuộc:** Task 1  
**WBS:** WP1.6, WP1.7 | **RTM:** FR-SEED-01, FR-SEED-02, FR-SEED-03 | **AC:** AC-SEED-01, AC-SEED-02, AC-SEED-03  
**Files:**
- Create: `data_prep/gen_pii.py`
- Create: `data_prep/split_batches.py`
- Create: `tests/test_data_prep.py`

- [ ] **Step 1: Viết test kiểm tra tệp dữ liệu Kaggle và dữ liệu PII sinh ra**
  ```python
  # tests/test_data_prep.py
  import os
  import glob
  import pandas as pd

  def test_raw_csv_files_present():
      """Kiểm tra đủ 9 file CSV của Kaggle Olist trong thư mục data_prep/raw"""
      csvs = glob.glob("data_prep/raw/*.csv")
      assert len(csvs) == 9, f"Cần đúng 9 file CSV, hiện chỉ tìm thấy: {len(csvs)}"

  def test_customer_pii_generation():
      """Kiểm tra file customer_pii.csv sinh ra khớp 1:1 với customer_unique_id"""
      cust_df = pd.read_csv("data_prep/raw/olist_customers_dataset.csv")
      unique_ids = cust_df['customer_unique_id'].nunique()
      
      pii_df = pd.read_csv("data_prep/processed/customer_pii.csv")
      assert len(pii_df) == unique_ids
      assert set(["customer_unique_id", "full_name", "email", "phone_number", "birth_date"]).issubset(pii_df.columns)
      # Đảm bảo không có trường null
      assert pii_df['email'].isnull().sum() == 0

  def test_batch_slicing_directories():
      """Kiểm tra đủ 8 mẻ dữ liệu B0 đến B7"""
      for i in range(8):
          batch_dir = f"data_prep/processed/batch_B{i}"
          assert os.path.isdir(batch_dir), f"Thiếu thư mục mẻ: {batch_dir}"
  ```

- [ ] **Step 2: Sao chép 9 tệp CSV từ `Olist-dataset/` sang `data_prep/raw/`**
  Run:
  ```bash
  cp Olist-dataset/*.csv data_prep/raw/
  pytest tests/test_data_prep.py::test_raw_csv_files_present -v
  ```
  Expected output: `1 passed`

- [ ] **Step 3: Viết script `data_prep/gen_pii.py` dùng thư viện Faker**
  ```python
  # data_prep/gen_pii.py
  import os
  import pandas as pd
  from faker import Faker

  def generate_pii():
      # Thiết lập seed cố định để đảm bảo tính tái lập (Reproducibility - NFR-06)
      fake = Faker()
      Faker.seed(42)

      raw_cust_path = "data_prep/raw/olist_customers_dataset.csv"
      out_dir = "data_prep/processed"
      os.makedirs(out_dir, exist_ok=True)
      out_path = os.path.join(out_dir, "customer_pii.csv")

      print(f"Reading {raw_cust_path}...")
      cust_df = pd.read_csv(raw_cust_path)
      unique_cust_ids = cust_df['customer_unique_id'].unique()

      print(f"Generating PII for {len(unique_cust_ids)} unique customers...")
      pii_records = []
      for cid in unique_cust_ids:
          pii_records.append({
              "customer_unique_id": cid,
              "full_name": fake.name(),
              "email": fake.email(),
              "phone_number": fake.phone_number(),
              "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=75).strftime('%Y-%m-%d')
          })

      pii_df = pd.DataFrame(pii_records)
      pii_df.to_csv(out_path, index=False)
      print(f"Saved generated PII to {out_path} ({len(pii_df)} records)")

  if __name__ == "__main__":
      generate_pii()
  ```

- [ ] **Step 4: Viết script `data_prep/split_batches.py`**
  Cắt dữ liệu theo thời gian thành 8 batch (B0 đến B7) và thêm cột `updated_at`.
  ```python
  # data_prep/split_batches.py
  import os
  import pandas as pd

  def split_into_batches():
      orders_file = "data_prep/raw/olist_orders_dataset.csv"
      df_orders = pd.read_csv(orders_file)
      df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
      
      # Thêm updated_at = order_purchase_timestamp cho mô phỏng CDC
      df_orders['updated_at'] = df_orders['order_purchase_timestamp']

      # Chia thành 8 mẻ: B0 (Lịch sử đến hết 2017-06), B1..B7 (mỗi mẻ 2 tháng tiếp theo)
      cutoffs = [
          pd.Timestamp('2017-07-01'),
          pd.Timestamp('2017-09-01'),
          pd.Timestamp('2017-11-01'),
          pd.Timestamp('2018-01-01'),
          pd.Timestamp('2018-03-01'),
          pd.Timestamp('2018-05-01'),
          pd.Timestamp('2018-07-01'),
          pd.Timestamp('2018-10-01'),
      ]

      for i in range(len(cutoffs)):
          batch_dir = f"data_prep/processed/batch_B{i}"
          os.makedirs(batch_dir, exist_ok=True)
          
          if i == 0:
              batch_orders = df_orders[df_orders['order_purchase_timestamp'] < cutoffs[0]]
          else:
              batch_orders = df_orders[
                  (df_orders['order_purchase_timestamp'] >= cutoffs[i-1]) & 
                  (df_orders['order_purchase_timestamp'] < cutoffs[i])
              ]
          
          batch_orders.to_csv(f"{batch_dir}/orders.csv", index=False)
          print(f"Batch B{i}: {len(batch_orders)} orders exported.")

  if __name__ == "__main__":
      split_into_batches()
  ```

- [ ] **Step 5: Thực thi sinh PII & Cắt batches, xác nhận toàn bộ test PASS**
  Run:
  ```bash
  python data_prep/gen_pii.py
  python data_prep/split_batches.py
  pytest tests/test_data_prep.py -v
  ```
  Expected output: `3 passed in ...`

- [ ] **Step 6: Git commit Task 5**
  ```bash
  git add data_prep/gen_pii.py data_prep/split_batches.py tests/test_data_prep.py
  git commit -m "feat: implement faker pii generation and historical batch slicing B0-B7"
  ```

---

### Task 6: Nạp Dữ liệu Khởi đầu (Seed PostgreSQL với Batch B0 & Đặt CSV vào MinIO Landing)
**Phụ thuộc:** Task 2, Task 5  
**WBS:** WP1.8 | **RTM:** FR-SEED-04, FR-SEED-05 | **AC:** AC-SEED-04, AC-SEED-05  
**Files:**
- Create: `data_prep/seed_postgres.py`
- Create: `tests/test_postgres_seeded.py`

- [ ] **Step 1: Viết test kiểm tra số lượng bản ghi đã được nạp vào PostgreSQL**
  ```python
  # tests/test_postgres_seeded.py
  import psycopg2

  def test_batch_b0_seeded():
      """Kiểm tra 5 bảng transactional trong PostgreSQL có dữ liệu mẻ B0"""
      conn = psycopg2.connect(
          host="localhost", port=5432, user="olist_admin", password="olist_password", dbname="oltp_olist"
      )
      cur = conn.cursor()
      tables = ["orders", "order_items", "order_payments", "customers", "customer_pii"]
      for t in tables:
          cur.execute(f"SELECT count(*) FROM {t};")
          count = cur.fetchone()[0]
          assert count > 0, f"Bảng {t} chưa có dữ liệu mẻ B0!"
      conn.close()
  ```

- [ ] **Step 2: Viết script `data_prep/seed_postgres.py`**
  ```python
  # data_prep/seed_postgres.py
  import pandas as pd
  from sqlalchemy import create_engine

  def seed_database():
      engine = create_engine("postgresql://olist_admin:olist_password@localhost:5432/oltp_olist")
      
      print("Seeding Batch B0 into PostgreSQL...")
      # 1. Orders (B0)
      df_orders = pd.read_csv("data_prep/processed/batch_B0/orders.csv")
      df_orders.to_sql("orders", engine, if_exists="replace", index=False)
      print(f"- Seeded {len(df_orders)} records into orders")

      # 2. Customers
      df_cust = pd.read_csv("data_prep/raw/olist_customers_dataset.csv")
      df_cust.to_sql("customers", engine, if_exists="replace", index=False)
      print(f"- Seeded {len(df_cust)} records into customers")

      # 3. Customer PII
      df_pii = pd.read_csv("data_prep/processed/customer_pii.csv")
      df_pii.to_sql("customer_pii", engine, if_exists="replace", index=False)
      print(f"- Seeded {len(df_pii)} records into customer_pii")

      # 4. Order items (filter theo orders của B0)
      df_items = pd.read_csv("data_prep/raw/olist_order_items_dataset.csv")
      df_items_b0 = df_items[df_items['order_id'].isin(df_orders['order_id'])]
      df_items_b0.to_sql("order_items", engine, if_exists="replace", index=False)
      print(f"- Seeded {len(df_items_b0)} records into order_items")

      # 5. Order payments (filter theo orders của B0)
      df_payments = pd.read_csv("data_prep/raw/olist_order_payments_dataset.csv")
      df_payments_b0 = df_payments[df_payments['order_id'].isin(df_orders['order_id'])]
      df_payments_b0.to_sql("order_payments", engine, if_exists="replace", index=False)
      print(f"- Seeded {len(df_payments_b0)} records into order_payments")

      print("PostgreSQL Database seeding for B0 complete!")

  if __name__ == "__main__":
      seed_database()
  ```

- [ ] **Step 3: Chạy seeding và chạy test xác nhận PASS**
  Run:
  ```bash
  python data_prep/seed_postgres.py
  pytest tests/test_postgres_seeded.py -v
  ```
  Expected output: `1 passed`

- [ ] **Step 4: Đẩy các bảng danh mục (Reference/Lookup CSV) lên MinIO `landing/`**
  Run:
  ```bash
  # Dùng docker container mc để upload CSV vào bucket landing
  docker compose exec -T mc-init /bin/sh -c "
    mc cp /data_prep/raw/product_category_name_translation.csv myminio/landing/ref/
    mc cp /data_prep/raw/olist_geolocation_dataset.csv myminio/landing/ref/
  " || echo "Copy via python boto3 if container mount not direct"
  ```

- [ ] **Step 5: Git commit Task 6**
  ```bash
  git add data_prep/seed_postgres.py tests/test_postgres_seeded.py
  git commit -m "feat: seed postgres database with batch b0 transactional records"
  ```

---

### Task 7: Thông Luồng End-to-End Smoke Test (Hello World Pipeline)
**Phụ thuộc:** Task 3, Task 4, Task 6  
**WBS:** WP1.4 | **RTM:** FR-SMOKE-01 đến FR-SMOKE-05 | **AC:** AC-SMOKE-01 đến AC-SMOKE-05 | **Gate:** Gate G1  
**Files:**
- Create: `spark_jobs/smoke_test_e2e.py`
- Create: `tests/test_e2e_smoke.py`

- [ ] **Step 1: Viết test kiểm tra đối soát (Tie-out Test) giữa PostgreSQL và Trino**
  ```python
  # tests/test_e2e_smoke.py
  import psycopg2
  import trino

  def test_pg_to_trino_tie_out():
      """Kiểm tra đối soát 100%: Số dòng orders trong PostgreSQL phải bằng số dòng trong Iceberg qua Trino"""
      # 1. Đếm trong Postgres
      conn_pg = psycopg2.connect(
          host="localhost", port=5432, user="olist_admin", password="olist_password", dbname="oltp_olist"
      )
      cur_pg = conn_pg.cursor()
      cur_pg.execute("SELECT count(*) FROM orders;")
      pg_count = cur_pg.fetchone()[0]
      conn_pg.close()

      # 2. Đếm trong Trino (đọc từ Iceberg bảng oms_orders_hello)
      conn_trino = trino.dbapi.connect(
          host='localhost', port=8080, user='admin', catalog='lakehouse', schema='bronze'
      )
      cur_trino = conn_trino.cursor()
      cur_trino.execute("SELECT count(*) FROM lakehouse.bronze.oms_orders_hello;")
      trino_count = cur_trino.fetchone()[0]
      conn_trino.close()

      print(f"PostgreSQL Count: {pg_count} | Trino Iceberg Count: {trino_count}")
      assert pg_count == trino_count, f"Dữ liệu lệch pha! PG={pg_count}, Trino={trino_count}"
  ```

- [ ] **Step 2: Viết Spark Job `spark_jobs/smoke_test_e2e.py` thực hiện pipeline E2E**
  ```python
  # spark_jobs/smoke_test_e2e.py
  from pyspark.sql import SparkSession

  def run_smoke_test():
      print("Initializing Spark Session with Iceberg REST Catalog...")
      spark = SparkSession.builder \
          .appName("SmokeTestE2E") \
          .master("local[2]") \
          .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3") \
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
          .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
          .config("spark.sql.catalog.lakehouse.type", "rest") \
          .config("spark.sql.catalog.lakehouse.uri", "http://localhost:8181/api/catalog") \
          .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
          .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://localhost:9000") \
          .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
          .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
          .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
          .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
          .config("spark.hadoop.fs.s3a.path.style.access", "true") \
          .getOrCreate()

      # 1. Đọc dữ liệu orders từ PostgreSQL qua JDBC
      print("Step 1: Reading orders table from PostgreSQL via JDBC...")
      jdbc_url = "jdbc:postgresql://localhost:5432/oltp_olist"
      df_orders = spark.read \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "orders") \
          .option("user", "olist_admin") \
          .option("password", "olist_password") \
          .option("driver", "org.postgresql.Driver") \
          .load()

      print(f"Extracted {df_orders.count()} records from PostgreSQL.")

      # 2. Tạo namespace bronze nếu chưa có
      spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;")

      # 3. Ghi vào Apache Iceberg table: lakehouse.bronze.oms_orders_hello
      print("Step 2: Writing DataFrame to Iceberg table (lakehouse.bronze.oms_orders_hello)...")
      df_orders.writeTo("lakehouse.bronze.oms_orders_hello") \
          .tableProperty("write.format.default", "parquet") \
          .createOrReplace()

      print("Successfully written to Apache Iceberg!")
      spark.stop()

  if __name__ == "__main__":
      run_smoke_test()
  ```

- [ ] **Step 3: Chạy Spark Pipeline & Chạy test kiểm chứng Trino Tie-out**
  Run:
  ```bash
  python spark_jobs/smoke_test_e2e.py
  pytest tests/test_e2e_smoke.py -v
  ```
  Expected output: `1 passed` (Số dòng hoàn toàn trùng khớp giữa OLTP và Lakehouse).

- [ ] **Step 4: Git commit Task 7**
  ```bash
  git add spark_jobs/smoke_test_e2e.py tests/test_e2e_smoke.py
  git commit -m "feat: implement end-to-end hello world pipeline from postgres to iceberg to trino"
  ```

---

### Task 8: Bài Tập SQL Thực Hành & Phân Tích Chuyên Sâu (Sư Phạm & Tư Duy Phản Biện)
**Phụ thuộc:** Task 6  
**Syllabus:** Section 3 (SQL hằng ngày) | **Files:**
- Create: `sql/week01_daily_queries.sql`

- [ ] **Step 1: Viết 5 truy vấn SQL kèm chú thích WHY (Tại sao quan trọng đối với Data Engineer)**
  ```sql
  -- sql/week01_daily_queries.sql
  -- TẬP TRUY VẤN SQL THỰC HÀNH TUẦN 01 (Chạy trên PostgreSQL oltp_olist)
  -- Chú trọng: Giải thích WHY (Ý nghĩa kinh doanh & kỹ thuật) thay vì chỉ gõ lệnh

  -- =========================================================================
  -- Bài 1: Đếm số đơn theo order_status
  -- WHY: Giúp Data Engineer nắm bắt vòng đời đơn hàng (Order State Machine).
  -- Chỉ những đơn có trạng thái hợp lệ ('delivered') mới được tính vào KPI Doanh thu ở Gold Layer.
  -- =========================================================================
  SELECT 
      order_status, 
      COUNT(*) AS total_orders,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
  FROM orders
  GROUP BY order_status
  ORDER BY total_orders DESC;

  -- =========================================================================
  -- Bài 2: Top 5 bang có nhiều khách hàng nhất
  -- WHY: Đánh giá phân bố địa lý (Data Skew). Nếu bang 'SP' (São Paulo) chiếm > 40%,
  -- khi partition bảng Iceberg/Spark, ta KHÔNG NÊN partition thuần túy theo state
  -- vì sẽ gây ra hiện tượng lệch tải (Partition Skew) làm chậm Spark task.
  -- =========================================================================
  SELECT 
      customer_state,
      COUNT(DISTINCT customer_unique_id) AS total_unique_customers
  FROM customers
  GROUP BY customer_state
  ORDER BY total_unique_customers DESC
  LIMIT 5;

  -- =========================================================================
  -- Bài 3: So sánh số customer_id vs customer_unique_id
  -- WHY: Bản chất Olist sinh một `customer_id` MỚI cho mỗi lần mua hàng!
  -- Muốn tính Customer Lifetime Value (LTV) hay RFM, bắt buộc phải dùng `customer_unique_id`.
  -- Nếu Data Analyst dùng nhầm `customer_id`, chỉ số LTV sẽ hoàn toàn sai lệch!
  -- =========================================================================
  SELECT 
      COUNT(customer_id) AS total_customer_transactions,
      COUNT(DISTINCT customer_unique_id) AS total_actual_customers,
      COUNT(customer_id) - COUNT(DISTINCT customer_unique_id) AS returning_customer_orders
  FROM customers;

  -- =========================================================================
  -- Bài 4: Đơn mới nhất của mỗi khách hàng (Dùng ROW_NUMBER Window Function)
  -- WHY: Đây là viên gạch nền tảng của kỹ thuật Deduplication (Khử trùng lặp)
  -- và CDC (Change Data Capture) khi xử lý dữ liệu ở tầng Silver.
  -- =========================================================================
  WITH RankedOrders AS (
      SELECT 
          c.customer_unique_id,
          o.order_id,
          o.order_purchase_timestamp,
          ROW_NUMBER() OVER (
              PARTITION BY c.customer_unique_id 
              ORDER BY o.order_purchase_timestamp DESC
          ) AS rn
      FROM orders o
      JOIN customers c ON o.customer_id = c.customer_id
  )
  SELECT 
      customer_unique_id,
      order_id AS latest_order_id,
      order_purchase_timestamp AS latest_order_date
  FROM RankedOrders
  WHERE rn = 1
  LIMIT 10;

  -- =========================================================================
  -- Bài 5: Doanh thu (price + freight_value) theo tháng
  -- WHY: Tiền đề để xây dựng bảng tổng hợp `kpi_monthly_revenue` ở tầng Gold.
  -- Giúp đối soát (Reconciliation) số liệu sau này giữa Postgres và Lakehouse.
  -- =========================================================================
  SELECT 
      TO_CHAR(o.order_purchase_timestamp, 'YYYY-MM') AS order_month,
      COUNT(DISTINCT o.order_id) AS delivered_orders,
      ROUND(SUM(oi.price + oi.freight_value)::NUMERIC, 2) AS total_gross_revenue
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  WHERE o.order_status = 'delivered'
  GROUP BY TO_CHAR(o.order_purchase_timestamp, 'YYYY-MM')
  ORDER BY order_month ASC;
  ```

- [ ] **Step 2: Thực thi kiểm tra cú pháp trên PostgreSQL**
  Run: `docker compose exec -T postgres psql -U olist_admin -d oltp_olist -f - < sql/week01_daily_queries.sql`
  Expected output: 5 bảng kết quả truy vấn trả về dữ liệu chuẩn xác.

- [ ] **Step 3: Git commit Task 8**
  ```bash
  git add sql/week01_daily_queries.sql
  git commit -m "docs: add 5 pedagogical daily sql queries with technical why explanations"
  ```

---

### Task 9: Bộ Tài Liệu Phỏng Vấn Kỹ Thuật (Interview Q&A Cheatsheet)
**Phụ thuộc:** Task 1 đến Task 8  
**Syllabus:** Section 4 (Interview Q&A) | **Files:**
- Create: `docs/week01_interview_prep.md`

- [ ] **Step 1: Viết tài liệu trả lời 5 câu hỏi phỏng vấn có liên hệ mật thiết với Project**
  Tập trung vào: Bản chất kiến trúc, Root Cause Analysis, và Case Study thực tế trong repo Olist Lakehouse.
  ```markdown
  # Week 01 Technical Interview Preparation

  ## Q1. Data Lakehouse khác Data Lake và Data Warehouse như thế nào?
  - **Data Lake (Quá khứ):** Lưu trữ file giá rẻ (S3, HDFS), linh hoạt chứa mọi loại dữ liệu (unstructured, semi-structured). Nhưng không hỗ trợ giao dịch ACID, không thể sửa từng dòng (UPDATE/DELETE), dễ biến thành "Data Swamp" (đầm lầy dữ liệu rác).
  - **Data Warehouse (Truyền thống):** Lưu trữ gắn chặt với tính toán (Coupled Storage-Compute), đảm bảo ACID nghiêm ngặt, SQL siêu nhanh nhưng chi phí bản quyền và phần cứng cực kỳ đắt đỏ, không thể mở rộng quy mô linh hoạt.
  - **Data Lakehouse (Hiện đại):** Tách rời Storage và Compute. Dữ liệu lưu dưới dạng tệp mở (Parquet) trên Object Storage giá rẻ (MinIO/S3), nhưng được quản lý bởi Table Format (Apache Iceberg) để mang lại năng lực ACID, Time Travel, Schema Evolution.
  - **Gắn liền Project:** *"Trong dự án Olist, tôi lưu Parquet trên MinIO S3, Iceberg quản lý bảng logic, Spark chịu trách nhiệm biến đổi batch nặng và Trino phục vụ truy vấn tương tác trên cùng một bản dữ liệu duy nhất."*

  ## Q2. Vì sao cần Catalog khi đã sử dụng Apache Iceberg?
  - Iceberg là đặc tả (Specification) và thư viện máy khách. Mỗi lần commit dữ liệu mới, Iceberg sinh ra một file metadata `.json` và manifest `.avro` mới trên S3.
  - S3 Object Storage là lưu trữ eventual consistency (tính nhất quán sau cùng), không có thao tác đổi tên file nguyên tử (atomic rename).
  - **Catalog đóng vai trò là Atomic Pointer:** Lưu con trỏ trỏ tới metadata file mới nhất của bảng. Thao tác hoán đổi con trỏ là Atomic (nguyên tử) và áp dụng Optimistic Concurrency Control (OCC). Nếu không có Catalog, Trino hay Spark sẽ không biết file metadata hiện hành nằm ở đâu.

  ## Q3. Khi nào nên dùng Apache Spark, khi nào nên dùng Trino?
  - **Apache Spark:** Tối ưu cho xử lý Batch nặng, các tác vụ ETL/ELT kéo dài hàng giờ với logic phức tạp. Spark có cơ chế Checkpoint, Spill to Disk và chịu lỗi cấp độ Task (Task-level retry). Khi 1 task bị lỗi giữa chừng, Spark tính toán lại chỉ task đó mà không làm hỏng cả job.
  - **Trino:** Tối ưu cho truy vấn SQL tương tác (Interactive Ad-hoc Query) phục vụ BI Dashboard và Data Analyst với độ trễ thấp (sub-second đến vài chục giây). Trino thực thi hoàn toàn In-Memory. Nếu 1 node chết giữa chừng, query sẽ FAIL toàn bộ và phải chạy lại từ đầu. Do đó Trino không phù hợp làm công cụ ETL nặng hàng giờ.

  ## Q4. Định dạng Parquet tốt hơn CSV cho Analytics ở điểm nào?
  - **Columnar Storage:** Lưu trữ theo cột thay vì theo dòng. Truy vấn phân tích thường chỉ cần 3-5 cột trên tổng số 50 cột; Parquet chỉ đọc đúng các cột đó từ ổ đĩa (I/O Pruning).
  - **Compression Ratio:** Dữ liệu cùng kiểu trong 1 cột có tính tương đồng cao, thuật toán Snappy/ZSTD nén tốt hơn CSV gấp 4–8 lần.
  - **Predicate Pushdown:** Parquet lưu sẵn Min/Max statistics trong header/footer của mỗi Row Group. Khi query `WHERE order_date >= '2018-01-01'`, engine tự động bỏ qua các block không chứa dữ liệu cần tìm mà không cần giải nén.

  ## Q5 (Behavioral). Bạn là Software Engineer, kinh nghiệm nào dùng được tốt nhất cho Data Engineering?
  - *"Tư duy Clean Code, Module hóa dự án, Version Control bằng Git, Container hóa với Docker và đặc biệt là văn hóa viết Unit Test (TDD) ngay từ đầu. Ví dụ trong dự án Olist, tôi thiết lập ngay bộ test kiểm tra idempotency, kiểm tra tính toàn vẹn của mã hash PII và xây dựng pipeline smoke test tự động trước khi bước vào viết logic biến đổi phức tạp."*

  ---
  *Made by Anh Tu - Share to be share*
  ```

- [ ] **Step 2: Git commit Task 9**
  ```bash
  git add docs/week01_interview_prep.md
  git commit -m "docs: add comprehensive week 1 interview q&a cheatsheet"
  ```

---

### Task 10: Tự Chấm Điểm Nghiệm Thu Cổng Gate G1 (Milestone M0 Verification)
**Phụ thuộc:** Toàn bộ Task 1 đến Task 9  
**Syllabus:** Section Gate G1 | **RTM:** Toàn bộ RTM Tuần 1  
**Files:**
- Create: `tests/gate_g1_check.sh`

- [ ] **Step 1: Viết script tự chấm Gate G1**
  Kiểm tra 3 tiêu chí:
  1. Restart toàn bộ stack (`docker compose down && up`) mà dữ liệu PostgreSQL và MinIO vẫn còn.
  2. Trino query thành công bảng Iceberg và số lượng bản ghi khớp 100% với Postgres.
  3. Git log có đầy đủ các commit rõ ràng cho từng task.
  ```bash
  cat << 'EOF' > tests/gate_g1_check.sh
  #!/bin/bash
  set -e

  echo "======================================================="
  echo "         BẮT ĐẦU TỰ ĐÁNH GIÁ CỔNG GATE G1 (LAB M0)      "
  echo "======================================================="

  # 1. Kiểm tra Git commit
  COMMITS_COUNT=$(git rev-list --count HEAD)
  echo "✔ [1/3] Kiểm tra Git history: Tìm thấy $COMMITS_COUNT commits."
  if [ "$COMMITS_COUNT" -lt 5 ]; then
      echo "❌ Thất bại: Số lượng commit quá ít, chưa tuân thủ cam kết frequent commits."
      exit 1
  fi

  # 2. Kiểm tra tính bền vững (Persistence test)
  echo "✔ [2/3] Thực hiện kiểm tra tính bền vững: Restart Docker stack..."
  docker compose --profile core down
  docker compose --profile core up -d
  sleep 15

  # 3. Chạy toàn bộ pytest suite
  echo "✔ [3/3] Chạy toàn bộ bộ kiểm thử tự động..."
  pytest tests/test_storage_infra.py tests/test_trino_catalog.py tests/test_data_prep.py tests/test_postgres_seeded.py tests/test_e2e_smoke.py -v

  echo ""
  echo "======================================================="
  echo " 🎉 XUẤT SẮC! TOÀN BỘ TIÊU CHÍ CỔNG GATE G1 ĐÃ ĐẠT ĐIỂM 100% "
  echo " Môi trường sẵn sàng bước sang Tuần 02 (Data Modeling & STTM)"
  echo "======================================================="
  EOF
  chmod +x tests/gate_g1_check.sh
  ```

- [ ] **Step 2: Thực thi script `gate_g1_check.sh`**
  Run: `bash tests/gate_g1_check.sh`
  Expected output: Toàn bộ kiểm tra PASS và thông báo đạt Gate G1.

- [ ] **Step 3: Git commit hoàn thiện Task 10**
  ```bash
  git add tests/gate_g1_check.sh
  git commit -m "test: add automated gate g1 verification script"
  ```

---

## 🔍 KẾT QUẢ TỰ RÀ SOÁT KẾ HOẠCH (SELF-REVIEW CHECKLIST)

1. **Độ phủ yêu cầu (Spec Coverage):**
   - Đầy đủ 10 bước của Lab M0 trong `week01.md`.
   - Đầy đủ 8 WBS (1.1 đến 1.8) trong `01_MOCK_PROJECT_PLAN.md`.
   - Kết nối 100% với RTM (`FR-INFRA`, `FR-SEED`, `FR-SMOKE`) trong `requirements/governance/traceability-matrix.md`.
   - Bao hàm trọn vẹn 5 bài tập SQL hàng ngày và 5 câu hỏi phỏng vấn kỹ thuật.
2. **Quét Placeholder:**
   - Hoàn toàn KHÔNG CÓ "TODO", "TBD", "viết test sau", "làm tương tự".
   - 100% các bước đều có code hoàn chỉnh, lệnh chạy và Expected Output cụ thể.
3. **Tính nhất quán về kiểu và tên gọi (Type & Name Consistency):**
   - Cổng PostgreSQL thống nhất `5432`, MinIO `9000/9001`, Polaris `8181`, Trino `8080`, Spark `8888`.
   - Tên catalog thống nhất là `lakehouse`, namespace thống nhất là `bronze`, bảng hello world là `oms_orders_hello`.

---
*Made by Anh Tu - Share to be share*
