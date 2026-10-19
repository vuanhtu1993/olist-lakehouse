# Module data_seeding — Data Schemas & Script Contracts

> **Parent Document:** [Module Spec](spec.md) | [Requirement Analysis Package](../../README.md)  
> **Module ID:** `DATA_SEED` | **Scope:** File Formats, Table Schemas & CLI Contracts  
> **Linked Documents:** [Acceptance Criteria](acceptance-criteria.md)

---

## 1. CLI Command Contracts

Bộ công cụ chuẩn bị dữ liệu trong thư mục `data_prep/`:

```bash
# 1. Sinh dữ liệu PII giả lập từ customers.csv
python data_prep/gen_pii.py \
  --input data_prep/raw/olist_customers_dataset.csv \
  --output data_prep/raw/customer_pii.csv

# 2. Phân chia 8 batch và thêm updated_at
python data_prep/split_batches.py \
  --raw-dir data_prep/raw \
  --out-dir data_prep/batches

# 3. Seed dữ liệu vào PostgreSQL OLTP
python data_prep/seed_postgres.py \
  --batch-dir data_prep/batches/batch_B0 \
  --db-url postgresql://postgres:postgres@localhost:5432/oltp_olist
```

---

## 2. Customer PII Data Contract (`customer_pii.csv`)

| Column Name | Data Type | Nullable | Description / Business Rule | Example |
|---|---|---|---|---|
| `customer_unique_id` | `VARCHAR(32)` | NO | Khóa chính, ánh xạ 1:1 từ bảng Olist customers | `861eff4711a542e4b93843c6dd7f44e3` |
| `full_name` | `VARCHAR(128)` | NO | Họ tên giả sinh bằng Faker (pt_BR) | `Rodrigo Silveira` |
| `email` | `VARCHAR(128)` | NO | Email hợp lệ sinh bằng Faker | `rodrigo.silveira@example.com` |
| `phone_number` | `VARCHAR(32)` | NO | Số điện thoại định dạng chuẩn Brazil | `+55 11 98765-4321` |
| `birth_date` | `DATE` | NO | Ngày sinh giả lập (18 đến 70 tuổi) | `1988-06-15` |
| `updated_at` | `TIMESTAMP` | NO | Thời điểm cập nhật bản ghi | `2018-01-01 00:00:00` |

---

## 3. Batch Slicing Contract (Mốc thời gian B0–B7)

| Batch | Khoảng thời gian `order_purchase_timestamp` | Loại dữ liệu | Mục đích kiểm thử |
|---|---|---|---|
| **B0** | `< 2018-01-01 00:00:00` | Giao dịch 2016–2017 (~45% dataset) | Initial Full Load ban đầu |
| **B1** | `2018-01-01` đến `2018-01-31 23:59:59` | Giao dịch tháng 01/2018 | Kiểm thử SCD Type 2 ở Tuần 4 |
| **B2** | `2018-02-01` đến `2018-02-28 23:59:59` | Giao dịch tháng 02/2018 | Kiểm thử Schema Drift (thêm `sales_channel`) ở Tuần 3 |
| **B3** | `2018-03-01` đến `2018-03-31 23:59:59` | Giao dịch tháng 03/2018 | Kiểm thử file reviews CSV bị lỗi format ở Tuần 3 |
| **B4** | `2018-04-01` đến `2018-04-30 23:59:59` | Giao dịch tháng 04/2018 | Kiểm thử Orphan Keys (trỏ product_id không tồn tại) ở Tuần 4 |
| **B5** | `2018-05-01` đến `2018-05-31 23:59:59` | Giao dịch tháng 05/2018 | Kiểm thử Data Quality Severity (payment âm, email sai) ở Tuần 4 |
| **B6** | `2018-06-01` đến `2018-06-30 23:59:59` | Giao dịch tháng 06/2018 (cài trùng B5) | Kiểm thử Idempotency & Deduplication ở Tuần 6 |
| **B7** | `2018-07-01` đến `2018-07-31 23:59:59` | Cần sửa và nạp lại tháng 03/2018 | Kiểm thử quy trình Backfill ở Tuần 6 |

---

## 4. PostgreSQL Table Schemas Contract (`oltp_olist`)

```sql
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(64),
    customer_state VARCHAR(2),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_pii (
    customer_unique_id VARCHAR(32) PRIMARY KEY,
    full_name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL,
    phone_number VARCHAR(32) NOT NULL,
    birth_date DATE NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES customers(customer_id),
    order_status VARCHAR(20) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(32) NOT NULL REFERENCES orders(order_id),
    order_item_id INT NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    seller_id VARCHAR(32) NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    freight_value NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS order_payments (
    order_id VARCHAR(32) NOT NULL REFERENCES orders(order_id),
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(20) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);
```

---
*Made by Anh Tu - Share to be share*
