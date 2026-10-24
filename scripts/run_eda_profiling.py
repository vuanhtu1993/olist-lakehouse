# scripts/run_eda_profiling.py
"""
Script tự động thực thi Exploratory Data Analysis (EDA) Profiling trên 2 nguồn dữ liệu:
1. Nguồn 1: PostgreSQL OLTP Database (5 bảng transactional B0)
2. Nguồn 2: MinIO S3 Landing Bucket (5 tệp CSV tham chiếu & đánh giá)

Đo lường toàn diện theo 5 Trụ Cột Kỹ Thuật (5 Pillars of Data Engineering EDA):
- Trụ cột 1: Schema Sizing & Storage Allocation (min_len, max_len, avg_len)
- Trụ cột 2: Domain Boundaries & Anomalies (min_val, max_val, zero_count, negative_count)
- Trụ cột 3: Temporal Consistency (min_date, max_date)
- Trụ cột 4: Key Distribution & Cardinality (null_pct, distinct_count, uniqueness_ratio)
- Trụ cột 5: Cross-Source Traceability & Referential Integrity

Kết xuất:
- docs/week02/eda_summary.json: Bảng siêu dữ liệu thống kê chi tiết từng cột
- notebooks/eda_profiling.ipynb: Jupyter Notebook phân tích trên Apache Spark & Postgres

Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os
import json
import time
import boto3
import psycopg2
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

DATETIME_COLUMNS = {
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "shipping_limit_date",
    "birth_date",
    "updated_at",
    "review_creation_date",
    "review_answer_timestamp"
}

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )

def get_s3_client():
    endpoint = "http://minio:9000" if os.path.exists("/.dockerenv") else "http://localhost:9000"
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def profile_postgres_table(cur, table_name):
    """Tính toán 5 Trụ Cột Kỹ Thuật trực tiếp từ PostgreSQL OLTP"""
    print(f"  → Đang profile PostgreSQL: {table_name}...")
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    row_count = cur.fetchone()[0]
    
    cur.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'public' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)
    columns_meta = cur.fetchall()
    
    col_stats = {}
    for col, dtype in columns_meta:
        # Xác định data_category
        if col in DATETIME_COLUMNS or "timestamp" in col or col.endswith("_date"):
            data_cat = "datetime"
        elif dtype in ('integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision', 'decimal'):
            data_cat = "numeric"
        elif dtype in ('boolean', 'bool'):
            data_cat = "boolean"
        else:
            data_cat = "string"
            
        # 1. Đo lường cơ bản: null count, distinct count
        cur.execute(f"""
            SELECT 
                COUNT(*) - COUNT({col}) AS null_count,
                COUNT(DISTINCT {col}) AS distinct_count
            FROM {table_name};
        """)
        null_cnt, dist_cnt = cur.fetchone()
        null_cnt = int(null_cnt)
        dist_cnt = int(dist_cnt)
        null_pct = round((null_cnt / row_count * 100), 2) if row_count > 0 else 0.0
        uniq_ratio = round((dist_cnt / row_count), 4) if row_count > 0 else 0.0
        
        stat = {
            "data_type": dtype,
            "data_category": data_cat,
            "null_count": null_cnt,
            "null_pct": float(null_pct),
            "distinct_count": dist_cnt,
            "uniqueness_ratio": float(uniq_ratio)
        }
        
        # 2. Đo lường chuyên sâu theo 5 Pillars
        if data_cat == "string":
            cur.execute(f"""
                SELECT 
                    COALESCE(MIN(LENGTH({col})), 0) AS min_l,
                    COALESCE(MAX(LENGTH({col})), 0) AS max_l,
                    COALESCE(ROUND(AVG(LENGTH({col})), 1), 0.0) AS avg_l
                FROM {table_name}
                WHERE {col} IS NOT NULL;
            """)
            min_l, max_l, avg_l = cur.fetchone()
            stat["min_len"] = int(min_l)
            stat["max_len"] = int(max_l)
            stat["avg_len"] = float(avg_l)
            
        elif data_cat == "numeric":
            cur.execute(f"""
                SELECT 
                    MIN({col}) AS min_v,
                    MAX({col}) AS max_v,
                    SUM(CASE WHEN {col} = 0 THEN 1 ELSE 0 END) AS zero_cnt,
                    SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END) AS neg_cnt
                FROM {table_name};
            """)
            min_v, max_v, zero_c, neg_c = cur.fetchone()
            stat["min_value"] = float(min_v) if min_v is not None else 0.0
            stat["max_value"] = float(max_v) if max_v is not None else 0.0
            stat["zero_count"] = int(zero_c or 0)
            stat["negative_count"] = int(neg_c or 0)
            
        elif data_cat == "datetime":
            cur.execute(f"""
                SELECT 
                    MIN(NULLIF({col}, '')) AS min_d,
                    MAX(NULLIF({col}, '')) AS max_d
                FROM {table_name};
            """)
            min_d, max_d = cur.fetchone()
            stat["min_date"] = str(min_d) if min_d else None
            stat["max_date"] = str(max_d) if max_d else None
            
        col_stats[col] = stat
        
    return {
        "source_type": "PostgreSQL",
        "source_origin": f"postgresql.public.{table_name}",
        "row_count": int(row_count),
        "column_count": len(columns_meta),
        "columns": col_stats
    }

def profile_s3_csv_file(s3_client, bucket, key, entity_name):
    """Tính toán 5 Trụ Cột Kỹ Thuật bằng cách stream trực tiếp từ MinIO S3 qua Boto3"""
    endpoint = s3_client.meta.endpoint_url
    s3_uri = f"s3://{bucket}/{key}"
    print(f"  → Đang profile MinIO Landing S3: {entity_name} ({s3_uri})...")
    
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    if "geolocation" in entity_name:
        df = pd.read_csv(obj['Body'], dtype={"geolocation_zip_code_prefix": str})
    else:
        df = pd.read_csv(obj['Body'])
        
    row_count = len(df)
    col_stats = {}
    
    for col in df.columns:
        null_cnt = int(df[col].isnull().sum())
        dist_cnt = int(df[col].nunique(dropna=True))
        null_pct = round((null_cnt / row_count * 100), 2) if row_count > 0 else 0.0
        uniq_ratio = round((dist_cnt / row_count), 4) if row_count > 0 else 0.0
        dtype_str = str(df[col].dtype)
        
        # Xác định data_category
        if col in DATETIME_COLUMNS or "timestamp" in col or col.endswith("_date"):
            data_cat = "datetime"
        elif pd.api.types.is_numeric_dtype(df[col]):
            data_cat = "numeric"
        elif pd.api.types.is_bool_dtype(df[col]):
            data_cat = "boolean"
        else:
            data_cat = "string"
            
        stat = {
            "data_type": dtype_str,
            "data_category": data_cat,
            "null_count": null_cnt,
            "null_pct": null_pct,
            "distinct_count": dist_cnt,
            "uniqueness_ratio": uniq_ratio
        }
        
        # 5 Pillars chi tiết
        if data_cat == "string":
            non_null = df[col].dropna().astype(str)
            if len(non_null) > 0:
                lens = non_null.str.len()
                stat["min_len"] = int(lens.min())
                stat["max_len"] = int(lens.max())
                stat["avg_len"] = round(float(lens.mean()), 1)
            else:
                stat["min_len"] = 0
                stat["max_len"] = 0
                stat["avg_len"] = 0.0
                
        elif data_cat == "numeric":
            non_null = df[col].dropna()
            stat["min_value"] = float(non_null.min()) if len(non_null) > 0 else 0.0
            stat["max_value"] = float(non_null.max()) if len(non_null) > 0 else 0.0
            stat["zero_count"] = int((df[col] == 0).sum())
            stat["negative_count"] = int((df[col] < 0).sum())
            
        elif data_cat == "datetime":
            non_null = df[col].dropna().astype(str)
            stat["min_date"] = str(non_null.min()) if len(non_null) > 0 else None
            stat["max_date"] = str(non_null.max()) if len(non_null) > 0 else None
            
        col_stats[col] = stat
        
    return {
        "source_type": "MinIO_Landing",
        "s3_uri": s3_uri,
        "s3_endpoint": endpoint,
        "row_count": row_count,
        "column_count": len(df.columns),
        "columns": col_stats
    }

def run_profiling():
    t_start = time.time()
    print("=" * 70)
    print("🚀 BẮT ĐẦU PROFILING 5 TRỤ CỘT KỸ THUẬT (POSTGRESQL + MINIO S3)")
    print("=" * 70)
    
    summary = {}
    
    # 1. Profile 5 bảng từ PostgreSQL OLTP
    print("\n[GIAI ĐOẠN 1/2] Phân tích 5 bảng transactional từ PostgreSQL OLTP:")
    conn = get_pg_connection()
    cur = conn.cursor()
    pg_tables = ["orders", "order_items", "order_payments", "customers", "customer_pii"]
    for tbl in pg_tables:
        summary[tbl] = profile_postgres_table(cur, tbl)
    conn.close()
    
    # 2. Profile 5 tệp CSV từ MinIO Landing S3 trực tiếp
    print("\n[GIAI ĐOẠN 2/2] Phân tích 5 tệp CSV stream trực tiếp từ MinIO S3 (s3://landing/):")
    s3 = get_s3_client()
    s3_sources = [
        ("products", "landing", "ref/olist_products_dataset.csv"),
        ("sellers", "landing", "ref/olist_sellers_dataset.csv"),
        ("order_reviews", "landing", "reviews/olist_order_reviews_dataset.csv"),
        ("geolocation", "landing", "ref/olist_geolocation_dataset.csv"),
        ("product_category_name_translation", "landing", "ref/product_category_name_translation.csv"),
    ]
    for entity, bucket, key in s3_sources:
        summary[entity] = profile_s3_csv_file(s3, bucket, key, entity)
        
    # 3. Xuất file JSON tổng hợp
    os.makedirs("docs/week02", exist_ok=True)
    summary_path = "docs/week02/eda_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"\n✔ Đã lưu kết quả tóm tắt profiling vào: {summary_path}")
    
    # 4. Sinh file Jupyter Notebook chuẩn sư phạm
    os.makedirs("notebooks", exist_ok=True)
    nb_path = "notebooks/eda_profiling.ipynb"
    generate_jupyter_notebook(summary, nb_path)
    print(f"✔ Đã khởi tạo Jupyter Notebook chuẩn sư phạm tại: {nb_path}")
    
    elapsed = time.time() - t_start
    print("=" * 70)
    print(f"🎉 HOÀN THÀNH EDA PROFILING CHUYÊN SÂU TRONG {elapsed:.1f} GIÂY!")
    print("=" * 70)

def generate_jupyter_notebook(summary, nb_path):
    """Sinh file Jupyter Notebook chuyên nghiệp cho sinh viên/kỹ sư thực hành trên Apache Spark"""
    cells = []

    def md_cell(text):
        lines = [l + "\n" for l in text.strip().split("\n")]
        if lines:
            lines[-1] = lines[-1].rstrip("\n")
        return {"cell_type": "markdown", "metadata": {}, "source": lines}

    def code_cell(code):
        lines = [l + "\n" for l in code.strip().split("\n")]
        if lines:
            lines[-1] = lines[-1].rstrip("\n")
        return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": lines}

    # 1. Header & Pedagogical Intro
    cells.append(md_cell("""# 📊 Olist Lakehouse — Exploratory Data Analysis (EDA) Profiling
> **Tác giả:** Anh Tú (IT Lecturer & Technical Writer)  
> **Phương pháp:** Pedagogical Approach — Chuyển hóa kiến thức phức tạp thành trực quan, dễ hiểu.  
> **Môi trường:** Apache Spark 3.5.0 + PostgreSQL 16 + MinIO S3 (chạy trên container `olist_spark_notebook:8888`)  
> **Bản quyền:** *Made by Anh Tu - Share to be share*  

---

## 💡 TẠI SAO DATA ENGINEER CẦN EDA CHUYÊN BIỆT?
Khác với **Data Science EDA** (tìm kiếm tương quan `correlation`, phân phối chuẩn để huấn luyện mô hình ML), **Data Engineering EDA** là quá trình khám nghiệm cấu trúc và ranh giới dữ liệu (**Structural & Boundary Inspection**) nhằm:
1. **Thiết lập Data Contract:** Định hình kích thước kiểu dữ liệu chính xác (VARCHAR sizing, tránh tràn bộ nhớ khi Spark shuffle).
2. **Xây dựng Data Quality Rule Catalog:** Không đặt bừa rule `NOT NULL` hay `> 0` làm sập pipeline (Pipeline Crash), mà xác lập đúng chính sách (*Quarantine, Log Warning, Unknown Value*).
3. **Phát hiện dị thường xuyên nguồn (Cross-Source Isolation):** Đo lường tính toàn vẹn tham chiếu giữa RDBMS OLTP và Object Storage Landing."""))

    # 2. Spark Session Initialization
    cells.append(md_cell("""## 1. Khởi Tạo SparkSession Phân Tán Kết Nối PostgreSQL JDBC & MinIO S3A
Spark đóng vai trò là Distributed Compute Engine kết nối đồng thời 2 nguồn:
- **Nguồn 1:** PostgreSQL OLTP qua JDBC Driver (`org.postgresql:postgresql:42.7.3`).
- **Nguồn 2:** MinIO S3 Landing qua S3A Filesystem (`org.apache.hadoop:hadoop-aws:3.3.4`)."""))

    cells.append(code_cell("""import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

packages = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3"
]

spark = SparkSession.builder \\
    .appName("Olist-EDA-Profiling-5Pillars") \\
    .master("local[2]") \\
    .config("spark.driver.memory", "1g") \\
    .config("spark.jars.packages", ",".join(packages)) \\
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \\
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")) \\
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")) \\
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✔ SparkSession đã khởi tạo thành công với S3A Connector và PostgreSQL JDBC Driver!")"""))

    # 3. Read PostgreSQL OLTP
    cells.append(md_cell("""## 2. Nguồn 1: Trích Xuất 5 Bảng Transactional Từ PostgreSQL OLTP
Kết nối Supabase Session Pooler qua Spark JDBC để trích xuất mẻ dữ liệu B0:
`orders`, `order_items`, `order_payments`, `customers`, `customer_pii`."""))

    cells.append(code_cell("""pg_host = os.getenv("POSTGRES_HOST", "aws-1-ap-southeast-2.pooler.supabase.com")
pg_port = os.getenv("POSTGRES_PORT", "5432")
pg_user = os.getenv("POSTGRES_USER", "postgres.iuvjhphnqpekptjzsifm")
pg_pwd = os.getenv("POSTGRES_PASSWORD", "olist-lakehouse")
pg_db = os.getenv("POSTGRES_DB", "postgres")

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

def load_pg_table(tbl_name):
    return spark.read \\
        .format("jdbc") \\
        .option("url", jdbc_url) \\
        .option("dbtable", tbl_name) \\
        .option("user", pg_user) \\
        .option("password", pg_pwd) \\
        .option("driver", "org.postgresql.Driver") \\
        .load()

df_orders = load_pg_table("orders")
df_items = load_pg_table("order_items")
df_payments = load_pg_table("order_payments")
df_customers = load_pg_table("customers")
df_pii = load_pg_table("customer_pii")

print(f"✔ Đọc thành công 5 bảng PostgreSQL OLTP:")
print(f"  • orders         : {df_orders.count():>8,} dòng")
print(f"  • order_items    : {df_items.count():>8,} dòng")
print(f"  • order_payments : {df_payments.count():>8,} dòng")
print(f"  • customers      : {df_customers.count():>8,} dòng")
print(f"  • customer_pii   : {df_pii.count():>8,} dòng")"""))

    # 4. Read MinIO S3 Landing
    cells.append(md_cell("""## 3. Nguồn 2: Trích Xuất 5 Tệp Tham Chiếu Từ MinIO Landing S3
Đọc trực tiếp các tệp CSV từ bucket `s3a://landing/` qua giao thức S3A phân tán:
`products`, `sellers`, `order_reviews`, `geolocation`, `translation`."""))

    cells.append(code_cell("""def load_s3_csv(key):
    return spark.read.option("header", "true").csv(f"s3a://landing/{key}")

df_products = load_s3_csv("ref/olist_products_dataset.csv")
df_sellers = load_s3_csv("ref/olist_sellers_dataset.csv")
df_reviews = load_s3_csv("reviews/olist_order_reviews_dataset.csv")
df_geo = load_s3_csv("ref/olist_geolocation_dataset.csv")
df_trans = load_s3_csv("ref/product_category_name_translation.csv")

print(f"✔ Đọc thành công 5 tệp CSV từ MinIO Landing S3:")
print(f"  • products     : {df_products.count():>10,} dòng")
print(f"  • sellers      : {df_sellers.count():>10,} dòng")
print(f"  • reviews      : {df_reviews.count():>10,} dòng")
print(f"  • geolocation  : {df_geo.count():>10,} dòng")
print(f"  • translations : {df_trans.count():>10,} dòng")"""))

    # 5. Pillar 1: Schema Sizing
    cells.append(md_cell("""## 4. Trụ Cột 1: Schema Sizing & Storage Footprint
Đo lường độ dài chuỗi (`min_len`, `max_len`, `avg_len`) để định hình chính xác kích thước VARCHAR, tránh dùng generic `TEXT` hoặc `VARCHAR(255)` gây lãng phí bộ nhớ khi Spark thực hiện shuffle join."""))

    cells.append(code_cell("""df_sizing = df_customers.select(
    F.min(F.length("customer_id")).alias("min_cust_id"),
    F.max(F.length("customer_id")).alias("max_cust_id"),
    F.min(F.length("customer_state")).alias("min_state"),
    F.max(F.length("customer_state")).alias("max_state"),
    F.round(F.avg(F.length("customer_city")), 1).alias("avg_city_len"),
    F.max(F.length("customer_city")).alias("max_city_len")
)
df_sizing.show()
print("💡 KHUYẾN NGHỊ DATA CONTRACT:")
print("  - customer_id    : VARCHAR(32) (Độ dài cố định chính xác 32 ký tự)")
print("  - customer_state : VARCHAR(2)  (Mã bang Brazil chuẩn 2 ký tự)")
print("  - customer_city  : VARCHAR(32) (Thành phố dài nhất 32 ký tự)")"""))

    # 6. Pillar 2: Domain Boundaries & Zero/Negative Anomalies
    cells.append(md_cell("""## 5. Trụ Cột 2: Domain Boundaries & Dị Thường Số Học
Khảo sát các trường số để phát hiện giá trị âm và các trường hợp thanh toán bằng 0 BRL."""))

    cells.append(code_cell("""df_anomalies = df_payments.select(
    F.min("payment_value").alias("min_payment"),
    F.max("payment_value").alias("max_payment"),
    F.sum(F.when(F.col("payment_value") == 0, 1).otherwise(0)).alias("zero_payment_count"),
    F.sum(F.when(F.col("payment_value") < 0, 1).otherwise(0)).alias("negative_payment_count")
)
df_anomalies.show()

# Truy vết cụ thể 9 bản ghi payment_value = 0
print("🔍 Chi tiết các giao dịch thanh toán 0 BRL (Khách hàng áp voucher giảm giá 100%):")
df_payments.filter(F.col("payment_value") == 0).show()"""))

    # 7. Pillar 3: Temporal Consistency & SLA
    cells.append(md_cell("""## 6. Trụ Cột 3: Temporal Consistency & Kiểm Tra Giao Trễ
Đo lường dải thời gian mua sắm và tỷ lệ đơn hàng bị giao trễ so với dự kiến (`order_delivered_customer_date > order_estimated_delivery_date`)."""))

    cells.append(code_cell("""df_delivered = df_orders.filter(
    (F.col("order_status") == "delivered") &
    F.col("order_delivered_customer_date").isNotNull() &
    F.col("order_estimated_delivery_date").isNotNull()
)

total_delivered = df_delivered.count()
late_delivered = df_delivered.filter(
    F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")
).count()

late_rate = round(late_delivered * 100.0 / total_delivered, 2)
print(f"Tổng số đơn giao thành công : {total_delivered:,}")
print(f"Số đơn bị giao trễ hẹn     : {late_delivered:,}")
print(f"Tỷ lệ giao trễ (SLA breach): {late_rate}%")"""))

    # 8. Pillar 4: Key Inflation
    cells.append(md_cell("""## 7. Trụ Cột 4: Cardinality & Hiện Tượng Thổi Phồng Khách Hàng
So sánh số lượng `customer_id` (mã giao dịch đơn) và `customer_unique_id` (cá nhân thực tế)."""))

    cells.append(code_cell("""total_cust_id = df_customers.select("customer_id").distinct().count()
total_unique_id = df_customers.select("customer_unique_id").distinct().count()
inflation_diff = total_cust_id - total_unique_id
inflation_pct = round(inflation_diff * 100.0 / total_cust_id, 2)

print(f"Số lượng transaction customer_id : {total_cust_id:,}")
print(f"Số lượng người dùng thực tế     : {total_unique_id:,}")
print(f"Chênh lệch mã khách lặp lại     : {inflation_diff:,} ({inflation_pct}%)")
print("🚨 LƯU Ý SỐNG CÒN KHI TÍNH LTV / RFM:")
print("  Bắt buộc phải GROUP BY theo customer_unique_id! Nếu dùng customer_id,")
print("  tần suất mua hàng (Frequency) của mọi khách hàng sẽ luôn bằng 1!")"""))

    # 9. Pillar 5: Cross-Source Integrity Check
    cells.append(md_cell("""## 8. Trụ Cột 5: Cross-Source Referential Integrity (Spark Join)
Kiểm tra xem các mặt hàng trong `order_items` (PostgreSQL) có chứa `product_id` nào không tồn tại trong danh mục `products` (MinIO S3) hay không."""))

    cells.append(code_cell("""# Thực hiện Left Anti Join giữa PostgreSQL order_items và MinIO Landing products
df_orphan_products = df_items.join(
    df_products,
    df_items["product_id"] == df_products["product_id"],
    "left_anti"
)

orphan_count = df_orphan_products.count()
print(f"Số dòng order_items mồ côi (không tìm thấy trong catalog S3): {orphan_count}")
if orphan_count > 0:
    print("Mẫu các mặt hàng mồ côi:")
    df_orphan_products.select("order_id", "order_item_id", "product_id", "price").show(5)
    print("💡 ĐỀ XUẤT PIPELINE:")
    print("  Ở tầng Gold mô hình chiều, ánh xạ các sản phẩm này về dim_product với product_sk = -1 (Unknown Product)!")"""))

    # 10. Conclusion & Footer
    cells.append(md_cell("""## 9. Tổng Kết & Khuyến Nghị Data Contract Cho Tầng Bronze/Silver
1. **Schema Sizing:** Cố định `VARCHAR(32)` cho các UUID/MD5 hash, `VARCHAR(2)` cho bang.
2. **Data Quality Rule:** Cho phép `payment_value = 0` (ngưỡng ≤ 0.01% mẻ nạp), cách ly các bản ghi âm tiền.
3. **Mô Hình Chiều Gold:** Ánh xạ khách hàng theo `customer_unique_id` (SCD Type 2) và gán `product_sk = -1` cho các sản phẩm khuyết thiếu.

---
*Made by Anh Tu - Share to be share*"""))

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.11.6"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

    with open(nb_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    run_profiling()
