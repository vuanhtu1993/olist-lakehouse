# tests/test_sources_ready.py
"""
Test kiểm thử hạ tầng 2 nguồn dữ liệu chuẩn Enterprise Lakehouse:
1. Nguồn 1 (PostgreSQL OLTP): Đúng 5 bảng transactional (orders, order_items, order_payments, customers, customer_pii)
   và tuyệt đối KHÔNG chứa các bảng reference/file bên ngoài.
2. Nguồn 2 (MinIO Object Storage): Bucket landing chứa đủ 5 tệp CSV (products, sellers, reviews, geolocation, translation).

Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os
import boto3
import psycopg2
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

def test_postgres_oltp_sources():
    """Kiểm tra PostgreSQL chỉ chứa đúng 5 bảng transactional, không chứa bảng file ngoài"""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )
    cur = conn.cursor()
    
    expected_oltp_tables = ["orders", "order_items", "order_payments", "customers", "customer_pii"]
    forbidden_tables = ["products", "sellers", "order_reviews", "geolocation", "product_category_name_translation"]
    
    print("\n[SOURCE 1: POSTGRESQL OLTP] Kiểm tra 5 bảng transactional:")
    for t in expected_oltp_tables:
        cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{t}');")
        exists = cur.fetchone()[0]
        assert exists, f"Bảng transactional '{t}' phải tồn tại trong PostgreSQL!"
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        cnt = cur.fetchone()[0]
        assert cnt > 0, f"Bảng '{t}' không được rỗng!"
        print(f"  ✔ Bảng {t:<20}: {cnt:>8,} dòng")
        
    for ft in forbidden_tables:
        cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{ft}');")
        exists = cur.fetchone()[0]
        assert not exists, f"VI PHẠM KIẾN TRÚC: Bảng file/reference '{ft}' không được lưu trong PostgreSQL OLTP!"
        
    conn.close()

def test_minio_landing_sources():
    """Kiểm tra MinIO bucket 'landing' có đầy đủ 5 tệp CSV nguồn"""
    endpoint = "http://minio:9000" if os.path.exists("/.dockerenv") else "http://localhost:9000"
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    expected_objects = [
        "ref/olist_products_dataset.csv",
        "ref/olist_sellers_dataset.csv",
        "ref/olist_geolocation_dataset.csv",
        "ref/product_category_name_translation.csv",
        "reviews/olist_order_reviews_dataset.csv"
    ]
    
    print("\n[SOURCE 2: MINIO S3 LANDING] Kiểm tra 5 tệp CSV:")
    for key in expected_objects:
        response = s3.head_object(Bucket="landing", Key=key)
        size = response['ContentLength']
        assert size > 0, f"Tệp s3://landing/{key} bị rỗng (0 bytes)!"
        print(f"  ✔ s3://landing/{key:<45}: {size:>10,} bytes")
