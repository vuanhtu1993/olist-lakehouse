# Global 03 — Domain Models, ERD & Glossary: Olist-Lakehouse-M0

> **Parent Document:** [Requirement Analysis Package](../README.md)  
> **Version:** 1.0 (Baseline)  
> **Architecture Layer:** `global/` (System-Wide Invariants)

---

## 1. Domain Glossary
* **OLTP (Online Transaction Processing):** Hệ thống cơ sở dữ liệu giao dịch nguồn (ở đây là PostgreSQL 16), tối ưu cho các thao tác đọc/ghi theo dòng (row-oriented).
* **Object Storage:** Hệ thống lưu trữ dữ liệu dạng tệp tin phi cấu trúc/bán cấu trúc (MinIO giả lập chuẩn AWS S3), tổ chức theo bucket và prefix key.
* **Table Format (Apache Iceberg):** Đặc tả cấu trúc dữ liệu quản lý danh sách file Parquet thông qua metadata hierarchy (Manifest List -> Manifest File -> Data File), hỗ trợ ACID và Time Travel.
* **REST Catalog:** Chuẩn giao diện mở của Iceberg để các compute engine truy vấn vị trí tệp tin metadata mới nhất thông qua HTTP/REST protocol (thực thi bởi Apache Polaris).
* **Smoke Test (Kiểm thử thông luồng):** Thao tác kiểm tra tích hợp tối giản nhằm bảo đảm toàn bộ các thành phần trong chuỗi mắt xích (Postgres -> Spark -> MinIO -> Trino) có thể giao tiếp thông suốt.

---

## 2. Entity Relationship Diagram (OLTP Source Database: `oltp_olist`)

Mô hình dữ liệu quan hệ được seed vào PostgreSQL trong giai đoạn M0:

```mermaid
erDiagram
    CUSTOMERS ||--|| CUSTOMER_PII : "has PII"
    CUSTOMERS ||--o{ ORDERS : "places"
    ORDERS ||--|{ ORDER_ITEMS : "contains"
    ORDERS ||--o{ ORDER_PAYMENTS : "paid by"

    CUSTOMERS {
        string customer_id PK "Khóa đơn hàng"
        string customer_unique_id "Khóa định danh người dùng thực tế"
        string customer_zip_code_prefix "Mã vùng"
        string customer_city "Thành phố"
        string customer_state "Bang (2 ký tự)"
    }

    CUSTOMER_PII {
        string customer_unique_id PK, FK "Khóa định danh người dùng"
        string full_name "Họ và tên giả lập"
        string email "Email giả lập"
        string phone_number "Số điện thoại giả lập"
        date birth_date "Ngày sinh giả lập"
        timestamp updated_at "Thời điểm cập nhật"
    }

    ORDERS {
        string order_id PK "Mã đơn hàng"
        string customer_id FK "Mã khách hàng gắn với đơn"
        string order_status "Trạng thái đơn (delivered, shipped,...)"
        timestamp order_purchase_timestamp "Thời điểm đặt hàng"
        timestamp order_approved_at "Thời điểm duyệt"
        timestamp order_delivered_carrier_date "Thời điểm giao bên vận chuyển"
        timestamp order_delivered_customer_date "Thời điểm khách nhận"
        timestamp order_estimated_delivery_date "Thời điểm dự kiến giao"
        timestamp updated_at "Thời điểm cập nhật bản ghi"
    }

    ORDER_ITEMS {
        string order_id PK, FK "Mã đơn hàng"
        int order_item_id PK "Số thứ tự sản phẩm trong đơn"
        string product_id "Mã sản phẩm"
        string seller_id "Mã người bán"
        timestamp shipping_limit_date "Hạn chót gửi hàng"
        decimal price "Giá bán sản phẩm"
        decimal freight_value "Phí vận chuyển"
        timestamp updated_at "Thời điểm cập nhật"
    }

    ORDER_PAYMENTS {
        string order_id PK, FK "Mã đơn hàng"
        int payment_sequential PK "Số thứ tự thanh toán"
        string payment_type "Phương thức thanh toán"
        int payment_installments "Số kỳ trả góp"
        decimal payment_value "Số tiền thanh toán"
        timestamp updated_at "Thời điểm cập nhật"
    }
```

---

## 3. MinIO Storage Bucket Specification

| Bucket Name | Quyền hạn | Mục đích sử dụng | Định dạng lưu trữ |
|---|---|---|---|
| `landing` | Read / Write | Chứa các file CSV nguồn tải từ Kaggle (Reviews, Products, Sellers, Geolocation) | Raw CSV files |
| `bronze` | Read / Write | Chứa Iceberg Table tầng Bronze (1:1 với nguồn, có thêm metadata tracking) | Parquet + Iceberg Metadata |
| `silver` | Read / Write | Chứa Iceberg Table tầng Silver (Làm sạch, SCD Type 2, kiểm tra chất lượng) | Parquet + Iceberg Metadata |
| `gold` | Read / Write | Chứa Iceberg Table tầng Gold (Star schema, KPI aggregates) | Parquet + Iceberg Metadata |

---
*Made by Anh Tu - Share to be share*
