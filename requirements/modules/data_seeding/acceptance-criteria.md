# Module data_seeding — Acceptance Criteria & Verification

> **Parent Document:** [Module Spec](spec.md) | [Data Schemas Contract](api-contract.md)  
> **Module ID:** `DATA_SEED` | **Standard:** Gherkin Behavioral Specification

---

## AC-SEED-01: Kiểm tra tính sẵn sàng của bộ dữ liệu gốc Kaggle
- **Mapped FR:** [`FR-SEED-01`](spec.md#2-functional-requirements)

```gherkin
Scenario: Bộ dữ liệu Olist được tải về đầy đủ
  Given thư mục "data_prep/raw/" đã được tạo
  When tôi kiểm tra danh sách tệp tin trong thư mục "data_prep/raw/"
  Then tôi quan sát thấy đủ 9 tệp tin CSV gốc của Olist (olist_orders, olist_customers, olist_order_items, olist_order_payments, olist_order_reviews, olist_products, olist_sellers, olist_geolocation, product_category_name_translation)
```

---

## AC-SEED-02: Dữ liệu PII sinh mới khớp 1:1 theo customer_unique_id
- **Mapped FR:** [`FR-SEED-02`](spec.md#2-functional-requirements)
- **Governing Rule:** [`BR-M0-03`](../../global/business-rules.md#br-m0-03)

```gherkin
Scenario: Sinh PII giả lập không trùng lặp và không có giá trị null
  Given tệp tin "data_prep/raw/olist_customers_dataset.csv" có N bản ghi distinct "customer_unique_id"
  When script "data_prep/gen_pii.py" thực thi hoàn tất
  Then tệp tin "data_prep/raw/customer_pii.csv" được tạo ra
  And tổng số dòng trong "customer_pii.csv" bằng đúng N
  And không có bất kỳ giá trị null nào ở các cột "full_name", "email", "phone_number", "birth_date"
  And toàn bộ email có định dạng regex hợp lệ "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
```

---

## AC-SEED-03: Phân chia 8 batch và thêm cột updated_at
- **Mapped FR:** [`FR-SEED-03`](spec.md#2-functional-requirements)
- **Governing Rule:** [`BR-M0-04`](../../global/business-rules.md#br-m0-04)

```gherkin
Scenario: Cắt batch chính xác theo thời gian và bổ sung updated_at
  Given dữ liệu thô và file customer_pii đã sẵn sàng
  When script "data_prep/split_batches.py" thực thi
  Then thư mục "data_prep/batches/" xuất hiện đủ 8 thư mục từ "batch_B0" đến "batch_B7"
  And trong "batch_B0/orders.csv", toàn bộ cột "order_purchase_timestamp" đều nhỏ hơn "2018-01-01 00:00:00"
  And mọi tệp tin CSV được tạo ra đều có thêm cột "updated_at" có kiểu dữ liệu timestamp hợp lệ
```

---

## AC-SEED-04: Nạp dữ liệu Batch B0 vào PostgreSQL thành công
- **Mapped FR:** [`FR-SEED-04`](spec.md#2-functional-requirements)

```gherkin
Scenario: Postgres DB oltp_olist được nạp đầy đủ 5 bảng giao dịch ban đầu
  Given PostgreSQL container đang chạy và database "oltp_olist" đã tạo schema
  When script "data_prep/seed_postgres.py" nạp batch B0
  Then truy vấn SQL "SELECT COUNT(*) FROM orders;" trả về kết quả > 0
  And truy vấn SQL "SELECT COUNT(*) FROM customer_pii;" khớp với số customer_unique_id trong B0
  And không có lỗi ràng buộc khóa ngoại (Foreign Key Violation) nào phát sinh
```

---

## AC-SEED-05: Đặt các file CSV tham chiếu vào MinIO Landing
- **Mapped FR:** [`FR-SEED-05`](spec.md#2-functional-requirements)

```gherkin
Scenario: MinIO bucket landing chứa các file snapshot tham chiếu
  Given MinIO container đang hoạt động
  When script copy dữ liệu landing hoàn tất
  Then trong bucket "landing/" quan sát thấy các file: "olist_products_dataset.csv", "olist_sellers_dataset.csv", "olist_geolocation_dataset.csv", "product_category_name_translation.csv"
```

---
*Made by Anh Tu - Share to be share*
