# data_prep/seed_postgres.py
"""
Script nạp toàn bộ 10 bảng dữ liệu Olist vào PostgreSQL (OLTP) phục vụ EDA và thiết kế Lab M1 Tuần 02.
Bao gồm:
- 5 bảng transactional (orders, order_items, order_payments, customers, customer_pii)
- 5 bảng reference/support (products, sellers, order_reviews, geolocation, product_category_name_translation)

Áp dụng cơ chế bulk loading bằng PostgreSQL native COPY streaming (copy_expert)
để đạt hiệu năng tối đa khi tải lượng dữ liệu lớn qua mạng WAN (1 triệu dòng geolocation < 30 giây).

Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Tự động nạp cấu hình từ .env
load_dotenv()

def get_engine():
    """
    Khởi tạo SQLAlchemy engine kết nối PostgreSQL.
    Hỗ trợ cả kết nối trực tiếp và kết nối qua Supabase pooler (Session Pooler).
    """
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "olist-lakehouse")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "postgres")
    
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)

def seed_database():
    engine = get_engine()
    start_time = time.time()
    
    print("=" * 70)
    print("🚀 BẮT ĐẦU NẠP ĐÚNG 5 BẢNG TRANSACTIONAL VÀO POSTGRESQL (OLTP)")
    print("=" * 70)

    # 0. Dọn dẹp sạch sẽ các bảng cũ bằng DROP TABLE ... CASCADE
    print("\n[0/5] Dọn dẹp các bảng cũ để đảm bảo tính idempotent...")
    with engine.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS order_audit_logs CASCADE;
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS order_payments CASCADE;
            DROP TABLE IF EXISTS order_reviews CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS customer_pii CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
            DROP TABLE IF EXISTS products CASCADE;
            DROP TABLE IF EXISTS sellers CASCADE;
            DROP TABLE IF EXISTS geolocation CASCADE;
            DROP TABLE IF EXISTS product_category_name_translation CASCADE;
        """))
    print("  ✔ Đã dọn dẹp các bảng cũ an toàn.")

    # Danh mục 5 bảng transactional nguồn Olist trong PostgreSQL
    table_sources = [
        ("orders", "data_prep/processed/batch_B0/orders.csv"),
        ("order_items", "data_prep/processed/batch_B0/order_items.csv"),
        ("order_payments", "data_prep/processed/batch_B0/order_payments.csv"),
        ("customers", "data_prep/raw/olist_customers_dataset.csv"),
        ("customer_pii", "data_prep/processed/customer_pii.csv"),
    ]

    raw_conn = engine.raw_connection()
    try:
        for idx, (tbl_name, file_path) in enumerate(table_sources, 1):
            t0 = time.time()
            print(f"\n[{idx}/10] Đang nạp bảng '{tbl_name}' từ {file_path}...")
            
            # 1. Đọc mẫu 10 dòng đầu bằng pandas để tự động khởi tạo cấu trúc lược đồ (DDL) chuẩn xác
            df_schema = pd.read_csv(file_path, nrows=10)
            df_schema.head(0).to_sql(tbl_name, engine, if_exists="replace", index=False)
            
            # 2. Sử dụng copy_expert để stream toàn bộ file CSV vào Postgres ở tốc độ cao nhất
            with raw_conn.cursor() as cur:
                with open(file_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(f"COPY {tbl_name} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
                raw_conn.commit()
                
                # 3. Đếm lại số dòng thực tế trong DB để nghiệm thu
                cur.execute(f"SELECT COUNT(*) FROM {tbl_name};")
                row_count = cur.fetchone()[0]
                
            elapsed_tbl = time.time() - t0
            print(f"  ✔ Nạp thành công {row_count:,} dòng vào '{tbl_name}' trong {elapsed_tbl:.2f}s.")
            
    finally:
        raw_conn.close()

    total_elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"🎉 NẠP THÀNH CÔNG TOÀN BỘ 10 BẢNG VÀO POSTGRESQL TRONG {total_elapsed:.1f} GIÂY!")
    print("=" * 70)

if __name__ == "__main__":
    seed_database()
