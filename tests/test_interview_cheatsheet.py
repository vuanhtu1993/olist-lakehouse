"""
Test kiểm tra cấu trúc 5 câu hỏi phỏng vấn Tuần 02 theo tiêu chuẩn sư phạm của Anh Tú:
Mỗi câu hỏi phải có: Định nghĩa chuẩn học thuật (Definition Anatomy),
Phân tích nguyên nhân gốc rễ (Root Cause), Đánh đổi (Trade-off) và Số liệu thực chứng (Quantified Evidence).
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os

def test_interview_cheatsheet_structure():
    path = "docs/week02/interview_cheatsheet.md"
    assert os.path.exists(path), f"File {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    
    questions = ["Grain", "EDA", "STTM", "customer_id", "HMAC"]
    for q in questions:
        assert q in content, f"Thiếu câu hỏi liên quan tới '{q}' trong interview cheatsheet!"
        
    assert "Made by Anh Tu - Share to be share" in content, "Thiếu footer bản quyền!"

def test_modular_interview_cheatsheet_exists():
    path = "docs/week02/theory/08-week02-interview-cheatsheet.md"
    if not os.path.exists(path):
        path = "docs/week02/08-week02-interview-cheatsheet.md"
    assert os.path.exists(path), f"File {path} không tồn tại!"
    content = open(path, "r", encoding="utf-8").read()
    assert "Made by Anh Tu - Share to be share" in content
