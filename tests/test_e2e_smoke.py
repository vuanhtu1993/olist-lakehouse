# tests/test_e2e_smoke.py
"""
Test đối soát dữ liệu (Reconciliation / Tie-out Test) End-to-End giữa:
- Hệ thống tác nghiệp nguồn: PostgreSQL (OLTP)
- Hệ thống đích: Apache Iceberg Table trên MinIO S3 truy vấn qua Trino

Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import psycopg2
import trino
from dotenv import load_dotenv

# Tự động nạp cấu hình từ .env
load_dotenv()

def test_pg_to_trino_tie_out():
    """
    Kiểm tra đối soát 100%: Số dòng orders trong PostgreSQL 
    phải bằng chính xác số dòng trong bảng Iceberg lakehouse.bronze.oms_orders_hello qua Trino.
    Độ lệch (Discrepancy) bắt buộc phải bằng 0.
    """
    # 1. Đếm số dòng bảng orders trong PostgreSQL
    pooler_url = os.getenv("POSTGRE_SESSION_POOLER")
    if pooler_url:
        conn_pg = psycopg2.connect(pooler_url)
    else:
        conn_pg = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
            dbname=os.getenv("POSTGRES_DB", "postgres")
        )
    cur_pg = conn_pg.cursor()
    cur_pg.execute("SELECT count(*) FROM orders;")
    pg_count = cur_pg.fetchone()[0]
    conn_pg.close()

    # 2. Đếm số dòng trong Trino (đọc từ bảng Iceberg oms_orders_hello)
    conn_trino = trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user=os.getenv("TRINO_USER", "admin"),
        catalog="lakehouse",
        schema="bronze"
    )
    cur_trino = conn_trino.cursor()
    cur_trino.execute("SELECT count(*) FROM lakehouse.bronze.oms_orders_hello")
    trino_count = cur_trino.fetchone()[0]
    conn_trino.close()

    discrepancy = abs(pg_count - trino_count)
    print(f"\n[RECONCILIATION TIE-OUT CHECK]")
    print(f"  ✔ PostgreSQL Orders Count:     {pg_count:>6,}")
    print(f"  ✔ Trino Iceberg Orders Count:  {trino_count:>6,}")
    print(f"  ✔ Discrepancy (Độ lệch):        {discrepancy:>6,}")

    assert pg_count == trino_count, (
        f"Dữ liệu lệch pha! PostgreSQL có {pg_count} dòng nhưng Trino Iceberg có {trino_count} dòng "
        f"(Độ lệch: {discrepancy})"
    )

if __name__ == "__main__":
    print(">>> Bắt đầu chạy test_pg_to_trino_tie_out()...")
    test_pg_to_trino_tie_out()
    print(">>> [PASS] Kiểm thử đối soát E2E hoàn toàn khớp 100%!")
