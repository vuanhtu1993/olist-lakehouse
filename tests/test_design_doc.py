"""
Test kiểm tra tài liệu thiết kế kiến trúc nạp dữ liệu và khóa nghiệp vụ (docs/week02/design.md):
- Kiểm tra tính đầy đủ của 10 bảng thực thể theo Mục 2 của 00_PROJECT_OVERVIEW.md.
- 5 bảng transactional nguồn PostgreSQL: orders, order_items, order_payments, customers, customer_pii.
- 5 bảng nguồn MinIO Landing S3: products, sellers, order_reviews, geolocation, product_category_name_translation.
- Mỗi bảng phải có đầy đủ: Nguồn đặt, Business Key, Load Pattern, Watermark Column, Change Detection Strategy, Độ nhạy cảm.
- Có bản quyền footer chuẩn sư phạm Anh Tú: Made by Anh Tu - Share to be share.
"""
import os

def test_design_document_exists():
    path = "docs/week02/design.md"
    assert os.path.exists(path), f"Tài liệu {path} không tồn tại!"

def test_design_document_tables_coverage():
    path = "docs/week02/design.md"
    assert os.path.exists(path), f"Tài liệu {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    
    postgres_tables = ["orders", "order_items", "order_payments", "customers", "customer_pii"]
    s3_tables = ["products", "sellers", "order_reviews", "geolocation", "product_category_name_translation"]
    
    for tbl in postgres_tables:
        assert tbl in content, f"Thiếu bảng PostgreSQL '{tbl}' trong tài liệu thiết kế!"
    for tbl in s3_tables:
        assert tbl in content, f"Thiếu bảng MinIO S3 '{tbl}' trong tài liệu thiết kế!"

def test_design_document_matrix_columns():
    path = "docs/week02/design.md"
    assert os.path.exists(path), f"Tài liệu {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    
    required_aspects = [
        "PostgreSQL",
        "MinIO",
        "Load Pattern",
        "Business Key",
        "Watermark",
        "Record Hash",
        "PII",
        "SCD",
        "Made by Anh Tu - Share to be share"
    ]
    for aspect in required_aspects:
        assert aspect.lower() in content.lower(), f"Thiếu nội dung thiết kế quan trọng: '{aspect}'!"

def test_design_document_patterns_alignment():
    path = "docs/week02/design.md"
    assert os.path.exists(path), f"Tài liệu {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    
    # Kiểm tra các load pattern cốt lõi đã được định danh
    patterns = ["Incremental", "Full snapshot", "CDC", "Full reload"]
    for p in patterns:
        assert p.lower() in content.lower(), f"Thiếu Load Pattern: '{p}'!"
