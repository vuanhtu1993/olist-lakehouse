# tests/test_postgres_seeded.py
"""
Test kiểm tra cơ sở dữ liệu tác nghiệp (OLTP PostgreSQL trên Supabase hoặc Local)
đã được nạp đầy đủ 5 bảng giao dịch ban đầu của mẻ dữ liệu B0.
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import psycopg2
from dotenv import load_dotenv

# Tự động nạp cấu hình từ .env
load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "olist-lakehouse"),
        dbname=os.getenv("POSTGRES_DB", "postgres")
    )

def test_batch_b0_seeded():
    """Kiểm tra 5 bảng transactional trong PostgreSQL đã có đầy đủ dữ liệu mẻ B0"""
    conn = get_connection()
    cur = conn.cursor()
    tables = ["orders", "order_items", "order_payments", "customers", "customer_pii"]
    
    counts = {}
    for t in tables:
        # Kiểm tra bảng có tồn tại và đếm số dòng
        cur.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name = '{t}'
            );
        """)
        exists = cur.fetchone()[0]
        assert exists, f"Bảng '{t}' chưa được tạo trong PostgreSQL!"
        
        cur.execute(f"SELECT count(*) FROM {t};")
        count = cur.fetchone()[0]
        counts[t] = count
        assert count > 0, f"Bảng '{t}' đang bị rỗng (0 dòng)!"
    
    print("\n[POSTGRES SEED CHECK] Số lượng bản ghi mẻ B0 đã nạp thành công:")
    for t, c in counts.items():
        print(f"  ✔ Bảng {t:<15}: {c:>6,} dòng")
        
    conn.close()
