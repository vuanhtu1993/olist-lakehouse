# tests/test_spark_iceberg.py
"""
Test khởi tạo SparkSession với Apache Iceberg Runtime và kết nối Polaris REST Catalog.
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import sys
import glob

# Tự động nạp PySpark từ SPARK_HOME nếu có
if "SPARK_HOME" in os.environ:
    spark_python = os.path.join(os.environ["SPARK_HOME"], "python")
    if spark_python not in sys.path:
        sys.path.insert(0, spark_python)
    py4j_zips = glob.glob(os.path.join(spark_python, "lib", "py4j*.zip"))
    for py4j_zip in py4j_zips:
        if py4j_zip not in sys.path:
            sys.path.insert(1, py4j_zip)

from pyspark.sql import SparkSession

def get_spark():
    """
    Khởi tạo SparkSession với:
    1. Iceberg Spark Runtime (tương thích Spark 3.5 và Scala 2.12)
    2. AWS S3 FileIO để đọc ghi trực tiếp lên MinIO
    3. REST Catalog trỏ về Polaris
    """
    # Tự động nhận diện chạy bên trong Docker hay trên Host
    is_docker = os.path.exists("/.dockerenv")
    default_polaris = "http://polaris:8181" if is_docker else "http://localhost:8181"
    default_minio = "http://minio:9000" if is_docker else "http://localhost:9000"

    polaris_uri = os.getenv("POLARIS_URI", default_polaris)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", default_minio)
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    builder = SparkSession.builder \
        .appName("OlistIcebergSpark") \
        .master("local[2]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.iceberg:iceberg-aws-bundle:1.6.0") \
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

def test_spark_creates_bronze_namespace():
    """Kiểm tra Spark có thể tạo và liệt kê namespace 'bronze' trên Iceberg REST catalog"""
    spark = get_spark()
    
    # 1. Tạo namespace bronze nếu chưa tồn tại
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze;")
    
    # 2. Lấy danh sách namespaces hiện có
    namespaces_df = spark.sql("SHOW NAMESPACES IN lakehouse;")
    namespaces = [row[0] for row in namespaces_df.collect()]
    
    # In ra terminal để học viên quan sát trực quan
    print(f"\n[SPARK] Danh sách namespaces trên Polaris Catalog: {namespaces}")
    
    assert "bronze" in namespaces, "Namespace 'bronze' chưa xuất hiện trong lakehouse catalog!"
    
    # Dọn dẹp session để giải phóng tài nguyên
    spark.stop()

if __name__ == "__main__":
    print(">>> Bắt đầu chạy test_spark_creates_bronze_namespace()...")
    test_spark_creates_bronze_namespace()
    print(">>> [PASS] Kiểm thử Spark kết nối Polaris REST Catalog và Iceberg thành công!")
