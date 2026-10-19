# data_prep/split_batches.py
"""
Script phân cắt dữ liệu giao dịch thành 8 mẻ lịch sử (Batch B0 đến B7)
và bổ sung trường updated_at để sẵn sàng cho mô phỏng CDC (Change Data Capture).
Tác giả: Anh Tú (Pedagogical Approach)
"""
import os
import pandas as pd

def split_into_batches():
    raw_dir = "data_prep/raw"
    processed_dir = "data_prep/processed"
    os.makedirs(processed_dir, exist_ok=True)

    print("[BATCH] Đang đọc các tệp giao dịch từ raw/...")
    df_orders = pd.read_csv(os.path.join(raw_dir, "olist_orders_dataset.csv"))
    df_items = pd.read_csv(os.path.join(raw_dir, "olist_order_items_dataset.csv"))
    df_payments = pd.read_csv(os.path.join(raw_dir, "olist_order_payments_dataset.csv"))
    df_reviews = pd.read_csv(os.path.join(raw_dir, "olist_order_reviews_dataset.csv"))

    # Chuyển đổi timestamp và tạo cột updated_at
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
    df_orders['updated_at'] = df_orders['order_purchase_timestamp']

    # 8 mốc cắt theo dòng thời gian (B0: Lịch sử ban đầu, B1..B7: Các đợt tăng trưởng 2 tháng/mẻ)
    cutoffs = [
        pd.Timestamp('2017-07-01'),  # B0: < 2017-07-01 (~13,000 đơn)
        pd.Timestamp('2017-09-01'),  # B1: 2017-07 -> 2017-08
        pd.Timestamp('2017-11-01'),  # B2: 2017-09 -> 2017-10
        pd.Timestamp('2018-01-01'),  # B3: 2017-11 -> 2017-12 (Mùa Black Friday)
        pd.Timestamp('2018-03-01'),  # B4: 2018-01 -> 2018-02
        pd.Timestamp('2018-05-01'),  # B5: 2018-03 -> 2018-04
        pd.Timestamp('2018-07-01'),  # B6: 2018-05 -> 2018-06
        pd.Timestamp('2018-10-18'),  # B7: 2018-07 -> hết
    ]

    print("\n[BATCH] Bắt đầu phân tách dữ liệu thành 8 mẻ (B0 - B7):")
    for i in range(len(cutoffs)):
        batch_dir = os.path.join(processed_dir, f"batch_B{i}")
        os.makedirs(batch_dir, exist_ok=True)

        if i == 0:
            batch_orders = df_orders[df_orders['order_purchase_timestamp'] < cutoffs[0]].copy()
        elif i == len(cutoffs) - 1:
            batch_orders = df_orders[df_orders['order_purchase_timestamp'] >= cutoffs[i - 1]].copy()
        else:
            batch_orders = df_orders[
                (df_orders['order_purchase_timestamp'] >= cutoffs[i - 1]) &
                (df_orders['order_purchase_timestamp'] < cutoffs[i])
            ].copy()

        # Lấy danh sách order_id của mẻ hiện tại để lọc các bảng con liên quan
        batch_order_ids = set(batch_orders['order_id'])
        batch_items = df_items[df_items['order_id'].isin(batch_order_ids)].copy()
        batch_payments = df_payments[df_payments['order_id'].isin(batch_order_ids)].copy()
        batch_reviews = df_reviews[df_reviews['order_id'].isin(batch_order_ids)].copy()

        # Định dạng timestamp lại thành chuỗi chuẩn ISO cho CSV
        batch_orders['order_purchase_timestamp'] = batch_orders['order_purchase_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        batch_orders['updated_at'] = batch_orders['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Thêm updated_at cho các bảng con tương ứng
        order_time_map = batch_orders.set_index('order_id')['updated_at'].to_dict()
        batch_items['updated_at'] = batch_items['order_id'].map(order_time_map)
        batch_payments['updated_at'] = batch_payments['order_id'].map(order_time_map)
        batch_reviews['updated_at'] = batch_reviews['order_id'].map(order_time_map)

        # Xuất CSV
        batch_orders.to_csv(os.path.join(batch_dir, "orders.csv"), index=False)
        batch_items.to_csv(os.path.join(batch_dir, "order_items.csv"), index=False)
        batch_payments.to_csv(os.path.join(batch_dir, "order_payments.csv"), index=False)
        batch_reviews.to_csv(os.path.join(batch_dir, "order_reviews.csv"), index=False)

        min_time = batch_orders['order_purchase_timestamp'].min()[:10] if len(batch_orders) > 0 else "N/A"
        max_time = batch_orders['order_purchase_timestamp'].max()[:10] if len(batch_orders) > 0 else "N/A"
        print(f"  ✔ Mẻ B{i}: {len(batch_orders):>5} đơn hàng ({min_time} -> {max_time}) | {len(batch_items):>5} items | {len(batch_payments):>5} payments")

    print("\n[BATCH] Hoàn thành phân tách 8 mẻ dữ liệu B0-B7 thành công!")

if __name__ == "__main__":
    split_into_batches()
