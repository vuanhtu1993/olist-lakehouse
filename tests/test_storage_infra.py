import os
import psycopg2
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# Tự động nạp các biến môi trường từ .env
load_dotenv()

def test_postgres_connection():
    """Kiểm tra PostgreSQL (Supabase Cloud hoặc Local) sẵn sàng nhận kết nối và đúng database"""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    assert "PostgreSQL" in version, f"Không phát hiện PostgreSQL! Trả về: {version}"
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
        assert b in bucket_names, f"Bucket '{b}' chưa được tạo trên MinIO!"
