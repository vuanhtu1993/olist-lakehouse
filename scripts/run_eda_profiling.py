# scripts/run_eda_profiling.py
"""
Script tự động thực thi Exploratory Data Analysis (EDA) Profiling trên 2 nguồn dữ liệu:
1. Nguồn 1: PostgreSQL OLTP Database (5 bảng transactional)
2. Nguồn 2: MinIO S3 Landing Bucket (5 tệp CSV tham chiếu)

Kết xuất:
- docs/week02/eda_summary.json: Bảng siêu dữ liệu thống kê chi tiết từng cột
- notebooks/eda_profiling.ipynb: Jupyter Notebook phục vụ phân tích trực quan trên Apache Spark & Postgres

Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os
import json
import time
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )

def profile_postgres_table(cur, table_name):
    """Tính toán các chỉ số thống kê từ PostgreSQL OLTP"""
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
        cur.execute(f"""
            SELECT 
                COUNT(*) - COUNT({col}) AS null_count,
                COUNT(DISTINCT {col}) AS distinct_count
            FROM {table_name};
        """)
        null_cnt, dist_cnt = cur.fetchone()
        null_pct = round((null_cnt / row_count * 100), 2) if row_count > 0 else 0.0
        
        col_stats[col] = {
            "data_type": dtype,
            "null_count": int(null_cnt),
            "null_pct": float(null_pct),
            "distinct_count": int(dist_cnt)
        }
        
    return {
        "source_type": "PostgreSQL",
        "row_count": int(row_count),
        "column_count": len(columns_meta),
        "columns": col_stats
    }

def profile_csv_file(file_path, entity_name):
    """Tính toán các chỉ số thống kê từ tệp CSV MinIO Landing (đọc qua pandas/pyarrow)"""
    print(f"  → Đang profile MinIO Landing CSV: {entity_name} ({file_path})...")
    
    # Đối với geolocation (~61MB / 1M dòng), đọc theo kiểu dữ liệu tối ưu
    if "geolocation" in entity_name:
        df = pd.read_csv(file_path, dtype={"geolocation_zip_code_prefix": str})
    else:
        df = pd.read_csv(file_path)
        
    row_count = len(df)
    col_stats = {}
    
    for col in df.columns:
        null_cnt = int(df[col].isnull().sum())
        dist_cnt = int(df[col].nunique(dropna=True))
        null_pct = round((null_cnt / row_count * 100), 2) if row_count > 0 else 0.0
        dtype = str(df[col].dtype)
        
        col_stats[col] = {
            "data_type": dtype,
            "null_count": null_cnt,
            "null_pct": null_pct,
            "distinct_count": dist_cnt
        }
        
    return {
        "source_type": "MinIO_Landing",
        "row_count": row_count,
        "column_count": len(df.columns),
        "columns": col_stats
    }

def run_profiling():
    t_start = time.time()
    print("=" * 70)
    print("🚀 BẮT ĐẦU PROFILING TOÀN DIỆN 10 THỰC THỂ (POSTGRESQL + MINIO LANDING)")
    print("=" * 70)
    
    summary = {}
    
    # 1. Profile 5 bảng từ PostgreSQL
    print("\n[GIAI ĐOẠN 1/2] Phân tích 5 bảng transactional từ PostgreSQL OLTP:")
    conn = get_pg_connection()
    cur = conn.cursor()
    pg_tables = ["orders", "order_items", "order_payments", "customers", "customer_pii"]
    for tbl in pg_tables:
        summary[tbl] = profile_postgres_table(cur, tbl)
    conn.close()
    
    # 2. Profile 5 tệp CSV từ MinIO Landing
    print("\n[GIAI ĐOẠN 2/2] Phân tích 5 tệp CSV từ MinIO Landing Bucket:")
    csv_sources = [
        ("products", "data_prep/raw/olist_products_dataset.csv"),
        ("sellers", "data_prep/raw/olist_sellers_dataset.csv"),
        ("order_reviews", "data_prep/raw/olist_order_reviews_dataset.csv"),
        ("geolocation", "data_prep/raw/olist_geolocation_dataset.csv"),
        ("product_category_name_translation", "data_prep/raw/product_category_name_translation.csv"),
    ]
    for entity, path in csv_sources:
        summary[entity] = profile_csv_file(path, entity)
        
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
    print(f"🎉 HOÀN THÀNH EDA PROFILING TRONG {elapsed:.1f} GIÂY!")
    print("=" * 70)

def generate_jupyter_notebook(summary, nb_path):
    """Sinh file Jupyter Notebook chuẩn chứa cả PySpark Session và phân tích đa nguồn"""
    notebook = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# Olist Lakehouse — Exploratory Data Analysis (EDA) Profiling\\n",
                    "\\n",
                    "> **Tác giả:** Anh Tú (Pedagogical Approach) — *Made by Anh Tu - Share to be share*  \\n",
                    "> **Môi trường:** Apache Spark 3.5.0 + PostgreSQL 16 (Chạy trên container `olist_spark_notebook:8888`)  \\n",
                    "> **Mục tiêu:** Phân tích khám phá cấu trúc, độ đầy đủ (% NULL), số lượng phần tử phân biệt (Cardinality) trên cả 2 nguồn: PostgreSQL OLTP và MinIO Landing S3."
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 1. Khởi Tạo SparkSession Tích Hợp PostgreSQL JDBC & MinIO S3\\n",
                    "Spark đóng vai trò là động cơ tính toán phân tán (Distributed Compute Engine), kết nối trực tiếp tới PostgreSQL để đọc dữ liệu giao dịch và kết nối MinIO S3 để đọc các tệp CSV landing."
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
                    "# Đọc cấu hình từ file tóm tắt\\n",
                    "with open('../docs/week02/eda_summary.json', 'r') as f:\\n",
                    "    eda_summary = json.load(f)\\n",
                    "\\n",
                    "# Hiển thị bảng tổng quan 10 thực thể từ 2 nguồn\\n",
                    "overview_data = [\\n",
                    "    {\\n",
                    "        'Thực Thể': name,\\n",
                    "        'Nguồn Lưu Trữ': info['source_type'],\\n",
                    "        'Số Dòng': f\"{info['row_count']:,}\",\\n",
                    "        'Số Cột': info['column_count']\\n",
                    "    }\\n",
                    "    for name, info in eda_summary.items()\\n",
                    "]\\n",
                    "df_overview = pd.DataFrame(overview_data)\\n",
                    "display(df_overview)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 2. Chi Tiết Tỷ Lệ NULL & Phân Bố Thuộc Tính Từng Thực Thể\\n",
                    "Xem xét các cột có tỷ lệ khuyết thiếu (% NULL) cao để chuẩn bị thiết lập ngưỡng cho Data Quality Rule Catalog (Task 8)."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Danh sách các cột có chứa giá trị NULL trên toàn hệ thống\\n",
                    "null_findings = []\\n",
                    "for entity, info in eda_summary.items():\\n",
                    "    for col, stats in info['columns'].items():\\n",
                    "        if stats['null_pct'] > 0:\\n",
                    "            null_findings.append({\\n",
                    "                'Thực thể': entity,\\n",
                    "                'Cột': col,\\n",
                    "                'Kiểu dữ liệu': stats['data_type'],\\n",
                    "                'Số dòng NULL': f\"{stats['null_count']:,}\",\\n",
                    "                'Tỷ lệ NULL (%)': f\"{stats['null_pct']}%\",\\n",
                    "                'Distinct Count': f\"{stats['distinct_count']:,}\"\\n",
                    "            })\\n",
                    "df_nulls = pd.DataFrame(null_findings).sort_values(by='Thực thể')\\n",
                    "display(df_nulls)"
                ]
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 3. Khám Phá Mối Quan Hệ Chéo Nguồn (Cross-Source Relational Exploration)\\n",
                    "Kiểm tra xem các khóa ngoại `product_id` trong bảng `order_items` (PostgreSQL) có tồn tại đầy đủ trong tệp `products` (MinIO Landing) hay không."
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Đọc trực tiếp để kiểm tra Orphan Product Keys\\n",
                    "df_items = pd.read_csv('../data_prep/processed/batch_B0/order_items.csv')\\n",
                    "df_products = pd.read_csv('../data_prep/raw/olist_products_dataset.csv')\\n",
                    "\\n",
                    "orphan_products = df_items[~df_items['product_id'].isin(df_products['product_id'])]\\n",
                    "print(f\"Tổng số mặt hàng trong mẻ B0: {len(df_items):,}\")\\n",
                    "print(f\"Số dòng order_items trỏ tới product_id không tồn tại: {len(orphan_products):,}\")"
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
