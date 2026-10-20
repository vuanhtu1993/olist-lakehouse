# spark_jobs/smoke_test_e2e.py
"""
Smoke Test Pipeline End-to-End (Hello World Pipeline):
Trích xuất bảng orders từ PostgreSQL (OLTP) qua JDBC và nạp vào
bảng Apache Iceberg lakehouse.bronze.oms_orders_hello trên MinIO S3 qua Polaris REST Catalog.

Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import sys

from pyspark.sql import SparkSession

def get_spark_session():
    """
    Khởi tạo SparkSession với:
    1. Iceberg Spark Runtime 3.5 & Iceberg AWS Bundle (S3FileIO)
    2. PostgreSQL JDBC Driver (42.7.3)
    3. Cấu hình kết nối Polaris REST Catalog và MinIO S3
    """
    is_docker = os.path.exists("/.dockerenv")
    default_polaris = "http://polaris:8181" if is_docker else "http://localhost:8181"
    default_minio = "http://minio:9000" if is_docker else "http://localhost:9000"

    polaris_uri = os.getenv("POLARIS_URI", default_polaris)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", default_minio)
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.6.0",
        "org.postgresql:postgresql:42.7.3"
    ]
    packages_str = ",".join(packages)

    print(f"[SPARK SETUP] Polaris URI: {polaris_uri}")
    print(f"[SPARK SETUP] MinIO Endpoint: {minio_endpoint}")

    builder = SparkSession.builder \
        .appName("SmokeTestE2E") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.jars.packages", packages_str) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "rest") \
        .config("spark.sql.catalog.lakehouse.uri", polaris_uri) \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://lakehouse/") \
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", minio_endpoint) \
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", aws_access_key) \
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", aws_secret_key) \
        .config("spark.sql.defaultCatalog", "lakehouse")

    return builder.getOrCreate()

def run_smoke_test():
    print(">>> ==========================================================")
    print(">>> BẮT ĐẦU CHẠY PIPELINE SMOKE TEST E2E (POSTGRES -> ICEBERG)")
    print(">>> ==========================================================")

    # 1. Khởi tạo Spark Session
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 2. Đọc cấu hình PostgreSQL từ .env (Ưu tiên POSTGRE_SESSION_POOLER nếu có)
    pooler_url = os.getenv("POSTGRE_SESSION_POOLER")
    if pooler_url:
        import urllib.parse
        parsed = urllib.parse.urlparse(pooler_url)
        pg_host = parsed.hostname
        pg_port = str(parsed.port or 5432)
        pg_user = parsed.username
        pg_password = parsed.password
        pg_db = parsed.path.lstrip('/')
        print(f"[POSTGRES CONFIG] Sử dụng Supabase Session Pooler IPv4: {pg_host}:{pg_port}")
    else:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_user = os.getenv("POSTGRES_USER", "postgres")
        pg_password = os.getenv("POSTGRES_PASSWORD", "olist-lakehouse")
        pg_db = os.getenv("POSTGRES_DB", "postgres")
        print(f"[POSTGRES CONFIG] Sử dụng Host trực tiếp: {pg_host}:{pg_port}")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    print(f"\n[BƯỚC 1] Trích xuất bảng 'orders' từ PostgreSQL qua JDBC...")
    print(f"         URL: jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}")

    df_orders = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "orders") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    record_count = df_orders.count()
    print(f"  ✔ Trích xuất thành công: {record_count:,} bản ghi từ PostgreSQL.")
    df_orders.printSchema()

    # 3. Tạo namespace bronze nếu chưa tồn tại
    print("\n[BƯỚC 2] Khởi tạo namespace 'bronze' trên Polaris REST Catalog...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;")

    # 4. Ghi dữ liệu vào bảng Apache Iceberg
    target_table = "lakehouse.bronze.oms_orders_hello"
    print(f"\n[BƯỚC 3] Ghi DataFrame vào bảng Iceberg: {target_table}...")
    df_orders.writeTo(target_table) \
        .tableProperty("write.format.default", "parquet") \
        .createOrReplace()

    print(f"  ✔ Đã ghi thành công {record_count:,} bản ghi vào {target_table} trên MinIO S3!")

    # 5. Đọc lại bảng Iceberg để kiểm tra snapshot
    print("\n[BƯỚC 4] Kiểm tra đọc trực tiếp từ Iceberg Catalog:")
    df_check = spark.read.table(target_table)
    iceberg_count = df_check.count()
    print(f"  ✔ Số dòng xác nhận từ Iceberg: {iceberg_count:,} bản ghi.")

    spark.stop()
    print("\n>>> PIPELINE SMOKE TEST E2E HOÀN TẤT THÀNH CÔNG!")
    print(">>> ==========================================================")

if __name__ == "__main__":
    run_smoke_test()
