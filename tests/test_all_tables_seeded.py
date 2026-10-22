# tests/test_all_tables_seeded.py
"""
Test kiểm thử cơ sở dữ liệu OLTP PostgreSQL đã được nạp đủ toàn bộ 10 bảng Olist
phục vụ cho công tác EDA toàn diện trong Lab M1.
Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )

def test_all_10_tables_seeded():
    """Kiểm tra toàn bộ 10 bảng Olist tồn tại và có dữ liệu > 0 dòng"""
    conn = get_connection()
    cur = conn.cursor()
    
    expected_tables = [
        "orders", "order_items", "order_payments", "customers", "customer_pii",
        "products", "sellers", "order_reviews", "geolocation", "product_category_name_translation"
    ]
    
    counts = {}
    for t in expected_tables:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = '{t}'
            );
        """)
        exists = cur.fetchone()[0]
        assert exists, f"LỖI: Bảng '{t}' chưa được tạo trong PostgreSQL!"
        
        cur.execute(f"SELECT count(*) FROM {t};")
        count = cur.fetchone()[0]
        counts[t] = count
        assert count > 0, f"LỖI: Bảng '{t}' tồn tại nhưng không có dữ liệu (0 dòng)!"
    
    print("\n[POSTGRES SEED CHECK] Số lượng bản ghi 10 bảng Olist:")
    for t, c in counts.items():
        print(f"  ✔ Bảng {t:<35}: {c:>8,} dòng")
    
    conn.close()
