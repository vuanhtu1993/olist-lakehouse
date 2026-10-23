# tests/test_eda_profiling.py
"""
Test kiểm thử EDA Profiling chuẩn Production Data Engineering:
1. Nguồn dữ liệu tách biệt chuẩn Enterprise Lakehouse:
   - 5 thực thể transactional: Đọc trực tiếp từ PostgreSQL OLTP.
   - 5 thực thể reference/reviews: Đọc trực tiếp từ MinIO S3 (s3://landing/...) qua S3 API/Boto3, không đọc local file!
2. Đo lường đầy đủ 5 Trụ Cột Kỹ Thuật (5 Pillars of DE EDA):
   - Trụ cột 1 (Schema Sizing): min_len, max_len cho trường chuỗi để tối ưu VARCHAR & phát hiện khoảng trắng.
   - Trụ cột 2 (Domain Boundaries): min_value, max_value, zero_count, negative_count cho trường số/tiền tệ.
   - Trụ cột 3 (Temporal Boundaries): min_date, max_date cho trường thời gian.
   - Trụ cột 4 (Key Distribution): distinct_count, null_pct, uniqueness_ratio.
   - Trụ cột 5 (Notebook Execution): Jupyter Notebook chứa mã PySpark đọc trực tiếp S3A và Postgres JDBC.
Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os
import json

def test_eda_notebook_and_output():
    nb_path = "notebooks/eda_profiling.ipynb"
    summary_path = "docs/week02/eda_summary.json"
    assert os.path.exists(nb_path), f"Notebook {nb_path} chưa được tạo!"
    assert os.path.exists(summary_path), f"Summary {summary_path} chưa được tạo!"
    
    with open(summary_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    expected_entities = [
        "orders", "order_items", "order_payments", "customers", "customer_pii",
        "products", "sellers", "order_reviews", "geolocation", "product_category_name_translation"
    ]
    
    for e in expected_entities:
        assert e in data, f"Thiếu thông tin profiling của '{e}' trong summary!"
        info = data[e]
        assert "source_type" in info, f"Thiếu nguồn lưu trữ (PostgreSQL / MinIO_Landing) cho '{e}'!"
        assert info["row_count"] > 0, f"Thực thể '{e}' không được có 0 dòng!"
        assert "columns" in info and len(info["columns"]) > 0, f"Thực thể '{e}' không có thông tin cột!"
        
        # Kiểm tra nhãn nguồn đúng kiến trúc
        if e in ["orders", "order_items", "order_payments", "customers", "customer_pii"]:
            assert info["source_type"] == "PostgreSQL", f"Thực thể '{e}' phải thuộc nguồn PostgreSQL!"
            assert "source_origin" in info and info["source_origin"].startswith("postgresql.public."), f"Thực thể '{e}' thiếu source_origin hợp lệ!"
        else:
            assert info["source_type"] == "MinIO_Landing", f"Thực thể '{e}' phải thuộc nguồn MinIO_Landing!"
            assert "s3_uri" in info and info["s3_uri"].startswith("s3://landing/"), f"Thực thể Landing '{e}' phải có thông tin s3_uri bắt đầu bằng s3://landing/!"
            assert "s3_endpoint" in info, f"Thực thể Landing '{e}' phải lưu endpoint S3 kết nối!"
        
        cols = info["columns"]
        for col_name, stats in cols.items():
            assert "null_pct" in stats, f"Cột {e}.{col_name} thiếu null_pct!"
            assert "distinct_count" in stats, f"Cột {e}.{col_name} thiếu distinct_count!"
            assert "data_category" in stats, f"Cột {e}.{col_name} thiếu phân loại data_category (string, numeric, datetime, boolean, other)!"
            assert "uniqueness_ratio" in stats, f"Cột {e}.{col_name} thiếu uniqueness_ratio!"
            
            # Kiểm tra 5 Pillars đo lường kỹ thuật
            cat = stats["data_category"]
            if cat == "string":
                assert "min_len" in stats and "max_len" in stats, f"Cột chuỗi {e}.{col_name} phải có min_len, max_len!"
                assert stats["min_len"] <= stats["max_len"], f"min_len > max_len ở {e}.{col_name}!"
            elif cat == "numeric":
                assert "min_value" in stats and "max_value" in stats, f"Cột số {e}.{col_name} phải có min_value, max_value!"
                assert "zero_count" in stats and "negative_count" in stats, f"Cột số {e}.{col_name} phải có zero_count, negative_count!"
                if stats["null_pct"] < 100.0:
                    assert stats["min_value"] <= stats["max_value"], f"min_value > max_value ở {e}.{col_name}!"
            elif cat == "datetime":
                assert "min_date" in stats and "max_date" in stats, f"Cột thời gian {e}.{col_name} phải có min_date, max_date!"

    # Kiểm tra nội dung Notebook có code PySpark S3A và Postgres JDBC
    nb_content = open(nb_path, "r", encoding="utf-8").read()
    assert "s3a://landing" in nb_content, "Notebook phải chứa code đọc dữ liệu từ s3a://landing!"
    assert "spark.read" in nb_content, "Notebook phải chứa code Spark DataFrame API!"
    assert "Made by Anh Tu - Share to be share" in nb_content, "Notebook thiếu footer bản quyền!"
