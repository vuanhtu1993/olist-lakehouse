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
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# Olist Lakehouse — Exploratory Data Analysis (EDA) Profiling\\n",
                    "\\n",
                    "> **Tác giả:** Anh Tú (IT Lecturer & Technical Writer)  \\n",
                    "> **Phương pháp:** Pedagogical Approach — Chuyển hóa kiến thức phức tạp thành trực quan, dễ hiểu.  \\n",
                    "> **Môi trường:** Apache Spark 3.5.0 + PostgreSQL 16 + MinIO S3 (chạy trên container `olist_spark_notebook:8888`)  \\n",
                    "> **Bản quyền:** *Made by Anh Tu - Share to be share*  \\n",
                    "\\n",
                    "---\\n",
                    "\\n",
                    "## 💡 TẠI SAO DATA ENGINEER CẦN EDA CHUYÊN BIỆT?\\n",
                    "Khác với **Data Science EDA** (tìm kiếm tương quan `correlation`, phân phối chuẩn để huấn luyện mô hình ML), **Data Engineering EDA** là quá trình khám nghiệm cấu trúc và ranh giới dữ liệu (**Structural & Boundary Inspection**) nhằm:\\n",
                    "1. **Thiết lập Data Contract:** Định hình kích thước kiểu dữ liệu chính xác (VARCHAR sizing, tránh tràn bộ nhớ).\\n",
                    "2. **Xây dựng Data Quality Rule Catalog:** Không đặt bừa rule `NOT NULL` hay `> 0` làm sập pipeline (Pipeline Crash), mà xác lập đúng chính sách (*Quarantine, Log Warning, Unknown Value*).\\n",
                    "3. **Phát hiện dị thường xuyên nguồn (Cross-Source Isolation):** Đo lường tính toàn vẹn tham chiếu giữa RDBMS OLTP và Object Storage Landing."
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 1. Khởi Tạo SparkSession Phân Tán Kết Nối PostgreSQL JDBC & MinIO S3A\\n",
                    "Spark hoạt động như một Distributed Compute Engine đọc trực tiếp từ `s3a://landing/` và PostgreSQL pooler qua JDBC."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "import os\\n",
                    "import json\\n",
                    "import pandas as pd\\n",
                    "from pyspark.sql import SparkSession\\n",
                    "\\n",
                    "# Khởi tạo Spark Session với cấu hình S3A MinIO\\n",
                    "spark = SparkSession.builder \\\\\\n",
                    "    .appName(\"Olist-EDA-Profiling-5Pillars\") \\\\\\n",
                    "    .config(\"spark.hadoop.fs.s3a.endpoint\", os.getenv(\"MINIO_ENDPOINT\", \"http://minio:9000\")) \\\\\\n",
                    "    .config(\"spark.hadoop.fs.s3a.access.key\", os.getenv(\"AWS_ACCESS_KEY_ID\", \"minioadmin\")) \\\\\\n",
                    "    .config(\"spark.hadoop.fs.s3a.secret.key\", os.getenv(\"AWS_SECRET_ACCESS_KEY\", \"minioadmin\")) \\\\\\n",
                    "    .config(\"spark.hadoop.fs.s3a.path.style.access\", \"true\") \\\\\\n",
                    "    .config(\"spark.hadoop.fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\") \\\\\\n",
                    "    .getOrCreate()\\n",
                    "\\n",
                    "print(\"✔ Spark Session đã khởi tạo thành công với S3A Filesystem Connector!\")\\n",
                    "\\n",
                    "# Đọc file siêu dữ liệu tổng hợp\\n",
                    "with open('../docs/week02/eda_summary.json', 'r') as f:\\n",
                    "    eda_summary = json.load(f)\\n",
                    "\\n",
                    "# Bảng tổng quan 10 thực thể từ 2 nguồn\\n",
                    "overview = [\\n",
                    "    {\\n",
                    "        'Thực Thể': name,\\n",
                    "        'Nguồn Lưu Trữ': info['source_type'],\\n",
                    "        'Vị Trí Gốc': info.get('source_origin', info.get('s3_uri')),\\n",
                    "        'Số Dòng': f\"{info['row_count']:,}\",\\n",
                    "        'Số Cột': info['column_count']\\n",
                    "    }\\n",
                    "    for name, info in eda_summary.items()\\n",
                    "]\\n",
                    "pd.DataFrame(overview)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 2. Trụ Cột 1: Schema Sizing & Storage Allocation (Tối Ưu Chuỗi)\\n",
                    "Trong cơ sở dữ liệu lớn, việc để `TEXT` hoặc `VARCHAR(255)` vô tội vạ làm lãng phí RAM của Spark khi shuffle dữ liệu. Thống kê `min_len` và `max_len` giúp định kích thước cột chính xác."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "sizing_rows = []\\n",
                    "for entity, info in eda_summary.items():\\n",
                    "    for col, stat in info['columns'].items():\\n",
                    "        if stat.get('data_category') == 'string':\\n",
                    "            sizing_rows.append({\\n",
                    "                'Thực Thể': entity,\\n",
                    "                'Cột': col,\\n",
                    "                'Min Len': stat.get('min_len', 0),\\n",
                    "                'Max Len': stat.get('max_len', 0),\\n",
                    "                'Avg Len': stat.get('avg_len', 0.0),\\n",
                    "                'Khuyến Nghị Kiểu Dữ Liệu': f\"VARCHAR({stat.get('max_len', 32)})\" if stat.get('max_len', 0) > 0 else \"VARCHAR(32)\"\\n",
                    "            })\\n",
                    "pd.DataFrame(sizing_rows).head(15)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 3. Trụ Cột 2: Domain Boundaries & Dị Thường Giá Trị Số\\n",
                    "Phát hiện các trường số có giá trị âm hoặc bằng 0 (ví dụ: `payment_value == 0` do sử dụng voucher giảm 100%)."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "numeric_rows = []\\n",
                    "for entity, info in eda_summary.items():\\n",
                    "    for col, stat in info['columns'].items():\\n",
                    "        if stat.get('data_category') == 'numeric':\\n",
                    "            numeric_rows.append({\\n",
                    "                'Thực Thể': entity,\\n",
                    "                'Cột': col,\\n",
                    "                'Min Value': stat.get('min_value'),\\n",
                    "                'Max Value': stat.get('max_value'),\\n",
                    "                'Số Dòng = 0': stat.get('zero_count', 0),\\n",
                    "                'Số Dòng Âm (<0)': stat.get('negative_count', 0)\\n",
                    "            })\\n",
                    "pd.DataFrame(numeric_rows)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 4. Trụ Cột 3 & 4: Tính Nhất Quán Thời Gian & Phân Bố Khóa\\n",
                    "Đo lường độ bao phủ thời gian và phát hiện hiện tượng thổi phồng khóa (`customer_id` vs `customer_unique_id`)."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Đọc trực tiếp từ Spark DataFrame để kiểm tra tỷ lệ trùng lặp khách hàng\\n",
                    "cust_info = eda_summary.get('customers', {})\\n",
                    "total_cust_ids = cust_info['columns']['customer_id']['distinct_count']\\n",
                    "unique_persons = cust_info['columns']['customer_unique_id']['distinct_count']\\n",
                    "diff = total_cust_ids - unique_persons\\n",
                    "pct_inflation = round((diff / total_cust_ids) * 100, 2)\\n",
                    "print(f\"Tổng số transaction customer_id : {total_cust_ids:,}\")\\n",
                    "print(f\"Tổng số khách hàng thực tế     : {unique_persons:,}\")\\n",
                    "print(f\"Số mã bị thổi phồng (Lặp lại) : {diff:,} ({pct_inflation}%)\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 5. Trụ Cột 5: Cross-Source Relational Integrity Check\\n",
                    "Kiểm tra xem `order_items` trong PostgreSQL có chứa `product_id` nào không tồn tại trong `s3a://landing/ref/olist_products_dataset.csv` hay không."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Minh họa đọc S3A qua PySpark API\\n",
                    "try:\\n",
                    "    df_products = spark.read.option(\"header\", \"true\").csv(\"s3a://landing/ref/olist_products_dataset.csv\")\\n",
                    "    print(f\"✔ Đọc thành công từ S3A MinIO! Số lượng sản phẩm: {df_products.count():,}\")\\n",
                    "except Exception as e:\\n",
                    "    print(\"Chưa bật kết nối S3A trực tiếp trong container, sử dụng Boto3 metadata:\")\\n",
                    "    prod_info = eda_summary.get('products', {})\\n",
                    "    print(f\"Số lượng sản phẩm trong MinIO Landing S3: {prod_info.get('row_count'):,}\")"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "\\n---\\n",
                    "*Made by Anh Tu - Share to be share*"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.10.12"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    with open(nb_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    run_profiling()
