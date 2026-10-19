# data_prep/upload_to_landing.py
"""
Script tải các tệp CSV tham chiếu/danh mục lên bucket landing/ref/ trên MinIO Object Storage.
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

def upload_ref_data():
    endpoint = "http://minio:9000" if os.path.exists("/.dockerenv") else "http://localhost:9000"
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    files_to_upload = [
        "product_category_name_translation.csv",
        "olist_geolocation_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv"
    ]

    print("\n[LANDING] Bắt đầu tải các tệp tham chiếu (Reference Data) lên s3://landing/ref/...")
    for filename in files_to_upload:
        filepath = os.path.join("data_prep/raw", filename)
        if os.path.exists(filepath):
            s3_key = f"ref/{filename}"
            size = os.path.getsize(filepath)
            print(f"  - Uploading {filename} ({size:,} bytes) -> s3://landing/{s3_key}...")
            s3.upload_file(filepath, "landing", s3_key)
            print(f"  ✔ Hoàn tất s3://landing/{s3_key}")
        else:
            print(f"  ⚠ Không tìm thấy {filepath}")

    print("\n[LANDING] Hoàn thành tải dữ liệu tham chiếu lên MinIO Landing Bucket!")

if __name__ == "__main__":
    upload_ref_data()
