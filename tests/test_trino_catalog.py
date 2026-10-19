# tests/test_trino_catalog.py
from dotenv import load_dotenv
import trino

load_dotenv()

def test_trino_shows_lakehouse_catalog():
    """Kiểm tra Trino query engine đã load được catalog lakehouse kết nối tới Iceberg REST"""
    conn = trino.dbapi.connect(
        host='localhost',
        port=8080,
        user='admin',
        catalog='system',
        schema='runtime'
    )
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cur.fetchall()]
    assert "lakehouse" in catalogs, f"Không tìm thấy 'lakehouse' catalog trong Trino! Đang có: {catalogs}"
    conn.close()
