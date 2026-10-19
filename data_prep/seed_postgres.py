# data_prep/seed_postgres.py
"""
Script nạp dữ liệu mẻ B0 vào cơ sở dữ liệu tác nghiệp (OLTP PostgreSQL trên Supabase).
Nạp 5 bảng transactional: orders, order_items, order_payments, customers, customer_pii.
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Tự động nạp biến môi trường từ .env
load_dotenv()

def get_engine():
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
    
    print("=" * 60)
    print("🚀 BẮT ĐẦU NẠP DỮ LIỆU KHỞI ĐẦU MẺ B0 VÀO POSTGRESQL (OLTP)")
    print("=" * 60)
    
    # 0. Dọn dẹp sạch sẽ các bảng cũ và ràng buộc Foreign Key
    print("\n[0/5] Dọn dẹp các bảng cũ bằng DROP TABLE ... CASCADE...")
    with engine.begin() as conn:
        from sqlalchemy import text
        conn.execute(text("""
            DROP TABLE IF EXISTS order_audit_logs CASCADE;
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS order_payments CASCADE;
            DROP TABLE IF EXISTS order_reviews CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS customer_pii CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
        """))
    print("  ✔ Đã dọn dẹp các bảng cũ sạch sẽ.")

    # 1. Orders (mẻ B0)
    print("\n[1/5] Đang nạp bảng 'orders' (mẻ B0)...")
    df_orders = pd.read_csv("data_prep/processed/batch_B0/orders.csv")
    df_orders.to_sql("orders", engine, if_exists="replace", index=False, chunksize=2000, method="multi")
    print(f"  ✔ Đã nạp {len(df_orders):,} dòng vào 'orders'.")

    # 2. Order Items (mẻ B0)
    print("\n[2/5] Đang nạp bảng 'order_items' (mẻ B0)...")
    df_items = pd.read_csv("data_prep/processed/batch_B0/order_items.csv")
    df_items.to_sql("order_items", engine, if_exists="replace", index=False, chunksize=2000, method="multi")
    print(f"  ✔ Đã nạp {len(df_items):,} dòng vào 'order_items'.")

    # 3. Order Payments (mẻ B0)
    print("\n[3/5] Đang nạp bảng 'order_payments' (mẻ B0)...")
    df_payments = pd.read_csv("data_prep/processed/batch_B0/order_payments.csv")
    df_payments.to_sql("order_payments", engine, if_exists="replace", index=False, chunksize=2000, method="multi")
    print(f"  ✔ Đã nạp {len(df_payments):,} dòng vào 'order_payments'.")

    # 4. Customers (Toàn bộ danh mục khách hàng ban đầu)
    print("\n[4/5] Đang nạp bảng 'customers'...")
    df_cust = pd.read_csv("data_prep/raw/olist_customers_dataset.csv")
    df_cust.to_sql("customers", engine, if_exists="replace", index=False, chunksize=2000, method="multi")
    print(f"  ✔ Đã nạp {len(df_cust):,} dòng vào 'customers'.")

    # 5. Customer PII (Thông tin định danh sinh giả lập)
    print("\n[5/5] Đang nạp bảng 'customer_pii'...")
    df_pii = pd.read_csv("data_prep/processed/customer_pii.csv")
    df_pii.to_sql("customer_pii", engine, if_exists="replace", index=False, chunksize=2000, method="multi")
    print(f"  ✔ Đã nạp {len(df_pii):,} dòng vào 'customer_pii'.")

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"🎉 NẠP THÀNH CÔNG TOÀN BỘ 5 BẢNG VÀO POSTGRESQL TRONG {elapsed:.1f} GIÂY!")
    print("=" * 60)

if __name__ == "__main__":
    seed_database()
