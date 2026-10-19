# tests/test_data_prep.py
"""
Bộ kiểm thử chuẩn bị dữ liệu: Kiểm tra tính sẵn sàng của 9 file CSV raw,
đối soát 1:1 dữ liệu giả lập PII và tính toàn vẹn của 8 mẻ dữ liệu B0-B7.
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import glob
import pandas as pd

def test_raw_csv_files_present():
    """Kiểm tra đủ 9 file CSV của Kaggle Olist trong thư mục data_prep/raw"""
    csvs = glob.glob("data_prep/raw/*.csv")
    assert len(csvs) == 9, f"Cần đúng 9 file CSV, hiện chỉ tìm thấy: {len(csvs)}"

def test_customer_pii_generation():
    """Kiểm tra file customer_pii.csv sinh ra khớp 1:1 với customer_unique_id"""
    cust_path = "data_prep/raw/olist_customers_dataset.csv"
    assert os.path.exists(cust_path), f"Không tìm thấy file {cust_path}"
    
    cust_df = pd.read_csv(cust_path)
    unique_ids = cust_df['customer_unique_id'].nunique()
    
    pii_path = "data_prep/processed/customer_pii.csv"
    assert os.path.exists(pii_path), f"Chưa sinh file {pii_path}"
    
    pii_df = pd.read_csv(pii_path)
    assert len(pii_df) == unique_ids, f"Số dòng PII ({len(pii_df)}) không khớp với customer_unique_id ({unique_ids})"
    
    required_cols = {"customer_unique_id", "full_name", "email", "phone_number", "birth_date"}
    assert required_cols.issubset(pii_df.columns), f"Thiếu cột PII bắt buộc! Đang có: {pii_df.columns.tolist()}"
    
    # Đảm bảo không có trường null
    assert pii_df['email'].isnull().sum() == 0, "Phát hiện email bị null!"
    assert pii_df['customer_unique_id'].nunique() == len(pii_df), "Phát hiện customer_unique_id bị trùng lặp!"

def test_batch_slicing_directories():
    """Kiểm tra đủ 8 mẻ dữ liệu B0 đến B7 và tính toàn vẹn của file orders.csv"""
    for i in range(8):
        batch_dir = f"data_prep/processed/batch_B{i}"
        assert os.path.isdir(batch_dir), f"Thiếu thư mục mẻ: {batch_dir}"
        order_file = os.path.join(batch_dir, "orders.csv")
        assert os.path.isfile(order_file), f"Thiếu file orders.csv trong mẻ: {batch_dir}"
        
        df = pd.read_csv(order_file)
        assert len(df) > 0, f"Mẻ {batch_dir} không có dòng đơn hàng nào!"
        assert "updated_at" in df.columns, f"Mẻ {batch_dir} thiếu trường updated_at phục vụ CDC!"
