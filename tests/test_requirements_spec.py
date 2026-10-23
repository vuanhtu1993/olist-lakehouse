# tests/test_requirements_spec.py
"""
Test kiểm tra định dạng tài liệu requirements.md theo tiêu chuẩn Agile/User Story.
Mỗi User Story phải có Acceptance Criteria đo lường được (Given-When-Then).
Tác giả: Anh Tú (Pedagogical Approach)
Made by Anh Tu - Share to be share
"""
import os

def test_requirements_document():
    path = "docs/week02/requirements.md"
    assert os.path.exists(path), f"File {path} không tồn tại!"
    
    content = open(path, "r", encoding="utf-8").read()
    assert len(content) > 1000, "Nội dung requirements.md quá ngắn!"
    
    # Kiểm tra có đủ 3 bài toán nghiệp vụ theo syllabus
    assert "US-01" in content and "Doanh thu" in content, "Thiếu User Story 01: Doanh thu"
    assert "US-02" in content and "LTV" in content, "Thiếu User Story 02: LTV (Customer Lifetime Value)"
    assert "US-03" in content and "RFM" in content, "Thiếu User Story 03: Phân khúc RFM"
    
    # Kiểm tra cấu trúc chuẩn Acceptance Criteria
    assert "Given" in content and "When" in content and "Then" in content, "Acceptance Criteria phải viết theo chuẩn Given-When-Then!"
    assert "Made by Anh Tu - Share to be share" in content, "Thiếu footer bản quyền tác giả Anh Tú"
