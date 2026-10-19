import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

def init_buckets():
    """Khởi tạo 4 bucket cốt lõi của Medallion Architecture trên MinIO S3"""
    endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    if "minio:" in endpoint:
        endpoint = "http://localhost:9000"
        
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        config=Config(signature_version='s3v4'),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )
    
    buckets = ["landing", "bronze", "silver", "gold", "lakehouse"]
    existing_buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    
    for bucket in buckets:
        if bucket not in existing_buckets:
            s3.create_bucket(Bucket=bucket)
            print(f"✔ Đã tạo bucket: {bucket}")
        else:
            print(f"ℹ Bucket '{bucket}' đã tồn tại.")

if __name__ == "__main__":
    init_buckets()
