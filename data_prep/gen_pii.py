# data_prep/gen_pii.py
"""
Script sinh dữ liệu định danh cá nhân giả lập (Synthetic PII Generation).
Sử dụng thư viện Faker với Seed cố định nhằm đảm bảo tính tái lập (Reproducibility - NFR-06).
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import pandas as pd
from faker import Faker

def generate_pii():
    # 1. Cố định Seed để mỗi lần chạy đều sinh ra đúng tập dữ liệu định danh giống nhau
    fake = Faker('pt_BR')  # Sử dụng locale Bồ Đào Nha phù hợp với ngữ cảnh E-commerce Brazil
    Faker.seed(42)

    raw_cust_path = "data_prep/raw/olist_customers_dataset.csv"
    out_dir = "data_prep/processed"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "customer_pii.csv")

    print(f"[PII] Đang đọc dữ liệu khách hàng từ {raw_cust_path}...")
    cust_df = pd.read_csv(raw_cust_path)
    unique_cust_ids = cust_df['customer_unique_id'].unique()

    print(f"[PII] Đang sinh thông tin PII giả lập cho {len(unique_cust_ids):,} khách hàng duy nhất...")
    pii_records = []
    for cid in unique_cust_ids:
        pii_records.append({
            "customer_unique_id": cid,
            "full_name": fake.name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=75).strftime('%Y-%m-%d')
        })

    pii_df = pd.DataFrame(pii_records)
    pii_df.to_csv(out_path, index=False)
    print(f"[PII] Đã xuất thành công {len(pii_df):,} bản ghi PII vào {out_path}!")

if __name__ == "__main__":
    generate_pii()
