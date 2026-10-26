"""
Test kiểm tra 5 phát hiện quan hệ đa nguồn (Cross-Source Relational Findings) của Tuần 02:
1. Finding 1 (Postgres OLTP): customer_id bị thổi phồng so với customer_unique_id (repeat customers ~ 3.36%).
2. Finding 2 (Cross-Source): order_items trong Postgres đối chiếu với products trong MinIO Landing S3.
3. Finding 3 (Postgres OLTP): Đơn hàng thanh toán nhiều phương thức/vouchers (tối đa 29 payments/đơn).
4. Finding 4 (Postgres OLTP): Tỷ lệ đơn giao trễ hạn so với estimated date (~4.59% đơn delivered).
5. Finding 5 (MinIO Landing S3): Độ trùng lặp tọa độ geolocation (1M dòng GPS vs 19k zip codes).
Và xác nhận tài liệu báo cáo docs/week02/eda_report.md tuân thủ chuẩn sư phạm Anh Tú.
"""
import os
import io
import boto3
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_postgres_conn():
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
        region_name='us-east-1'
    )

def test_finding_1_customer_key_inflation():
    """Finding 1: Kiểm chứng độ lệch giữa customer_id và customer_unique_id"""
    conn = get_postgres_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(customer_id), count(distinct customer_id), count(distinct customer_unique_id) FROM customers;")
    total_cid, dist_cid, dist_uid = cur.fetchone()
    conn.close()
    
    assert total_cid == dist_cid, "customer_id phải là duy nhất trong bảng customers nguồn"
    assert total_cid > dist_uid, "Tổng customer_id phải lớn hơn customer_unique_id do có khách mua lại"
    repeat_customers = total_cid - dist_uid
    inflation_pct = (repeat_customers / total_cid) * 100
    assert repeat_customers > 3000, f"Số khách hàng mua lại phải > 3000 (thực tế: {repeat_customers})"
    print(f"\n✔ Finding 1 Verified: {total_cid:,} customer_id vs {dist_uid:,} unique persons -> {repeat_customers:,} repeat ({inflation_pct:.2f}%)")

def test_finding_2_cross_source_product_coverage():
    """Finding 2: Đối chiếu khóa ngoại liên nguồn order_items (Postgres) và products (MinIO S3)"""
    conn = get_postgres_conn()
    cur = conn.cursor()
    cur.execute("SELECT distinct product_id FROM order_items;")
    pg_products = set(r[0] for r in cur.fetchall())
    conn.close()
    
    s3 = get_s3_client()
    resp = s3.get_object(Bucket="landing", Key="ref/olist_products_dataset.csv")
    products_df = pd.read_csv(io.BytesIO(resp['Body'].read()))
    s3_products = set(products_df['product_id'].dropna().unique())
    
    missing_in_s3 = pg_products - s3_products
    assert len(missing_in_s3) == 0, f"Có {len(missing_in_s3)} product_id trong Postgres không có trong MinIO S3!"
    print(f"\n✔ Finding 2 Verified: 100% referential integrity ({len(pg_products):,} products in orders exist in S3 catalog of {len(s3_products):,} products)")

def test_finding_3_order_payments_fanout():
    """Finding 3: Kiểm chứng đơn hàng thanh toán nhiều lần và voucher 0 đồng"""
    conn = get_postgres_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT max(cnt), count(*) FROM (
            SELECT order_id, count(*) as cnt FROM order_payments GROUP BY order_id
        ) t WHERE cnt > 1;
    """)
    max_pay_cnt, multi_pay_orders = cur.fetchone()
    
    cur.execute("SELECT count(*) FROM order_payments WHERE payment_value = 0;")
    zero_val_count = cur.fetchone()[0]
    conn.close()
    
    assert max_pay_cnt >= 20, f"Đơn hàng thanh toán nhiều nhất phải có ít nhất 20 phương thức (thực tế: {max_pay_cnt})"
    assert multi_pay_orders > 500, f"Số đơn thanh toán nhiều lần phải > 500 (thực tế: {multi_pay_orders})"
    assert zero_val_count > 0, "Phải có giao dịch thanh toán 0 đồng do voucher"
    print(f"\n✔ Finding 3 Verified: {multi_pay_orders:,} multi-payment orders, max {max_pay_cnt} payments/order, {zero_val_count} zero-value vouchers")

def test_finding_4_late_delivery_orders():
    """Finding 4: Kiểm chứng tỷ lệ giao hàng trễ hạn so với estimated date"""
    conn = get_postgres_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            count(*) as total_delivered,
            count(*) filter (where order_delivered_customer_date > order_estimated_delivery_date) as late_orders
        FROM orders 
        WHERE order_status = 'delivered' and order_delivered_customer_date is not null;
    """)
    total_del, late_del = cur.fetchone()
    conn.close()
    
    assert total_del > 10000, "Số đơn delivered phải > 10,000"
    late_pct = (late_del / total_del) * 100
    assert 3.0 < late_pct < 10.0, f"Tỷ lệ trễ hạn phải nằm trong khoảng 3% - 10% (thực tế: {late_pct:.2f}%)"
    print(f"\n✔ Finding 4 Verified: {late_del:,} / {total_del:,} delivered orders were late ({late_pct:.2f}%)")

def test_finding_5_geolocation_redundancy():
    """Finding 5: Kiểm chứng độ trùng lặp tọa độ trong geolocation landing"""
    s3 = get_s3_client()
    resp = s3.get_object(Bucket="landing", Key="ref/olist_geolocation_dataset.csv")
    geo_df = pd.read_csv(io.BytesIO(resp['Body'].read()))
    total_rows = len(geo_df)
    unique_zips = geo_df['geolocation_zip_code_prefix'].nunique()
    
    assert total_rows >= 1000000, f"Tổng số dòng GPS phải >= 1M (thực tế: {total_rows:,})"
    assert unique_zips < 25000, f"Số zip code duy nhất phải < 25,000 (thực tế: {unique_zips:,})"
    ratio = total_rows / unique_zips
    assert ratio > 40.0, f"Độ trùng lặp trung bình phải > 40 lần/zip (thực tế: {ratio:.1f})"
    print(f"\n✔ Finding 5 Verified: {total_rows:,} raw GPS pings for {unique_zips:,} zip codes ({ratio:.1f}x redundancy)")

def test_eda_report_document_structure():
    """Kiểm tra sự tồn tại và cấu trúc sư phạm của docs/week02/eda_report.md"""
    path = "docs/week02/eda_report.md"
    assert os.path.exists(path), f"File {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    
    required_keywords = [
        "customer_unique_id",
        "product_id",
        "order_payments",
        "order_estimated_delivery_date",
        "geolocation_zip_code_prefix",
        "Quantified Evidence",
        "Root Cause",
        "Business Impact",
        "Pipeline Action",
        "Made by Anh Tu - Share to be share"
    ]
    for kw in required_keywords:
        assert kw in content, f"Báo cáo EDA thiếu từ khóa/phần cấu trúc bắt buộc: '{kw}'"
