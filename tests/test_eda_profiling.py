# tests/test_eda_profiling.py
"""
Test kiểm tra script/notebook EDA Profiling đã tính toán đầy đủ các chỉ số
cho toàn bộ 10 thực thể từ cả 2 nguồn: PostgreSQL OLTP và MinIO Landing S3.
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
        assert "row_count" in info and info["row_count"] > 0, f"Bảng '{e}' có row_count không hợp lệ!"
        assert "columns" in info and len(info["columns"]) > 0, f"Bảng '{e}' không có thông tin cột!"
        
        for col, stats in info["columns"].items():
            assert "null_pct" in stats, f"Cột {col} của {e} thiếu chỉ số null_pct"
            assert "distinct_count" in stats, f"Cột {col} của {e} thiếu chỉ số distinct_count"
            assert 0.0 <= stats["null_pct"] <= 100.0, f"null_pct của {col} không hợp lệ!"
    
    print("\n[EDA PROFILING CHECK] Đã kiểm chứng đầy đủ 10 thực thể từ 2 nguồn:")
    for e in expected_entities:
        info = data[e]
        print(f"  ✔ [{info['source_type']:<13}] {e:<35}: {info['row_count']:>10,} dòng | {len(info['columns']):>2} cột")
