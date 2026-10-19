# Global 02 — Core Business Rules & Invariants: Olist-Lakehouse-M0

> **Parent Document:** [Requirement Analysis Package](../README.md)  
> **Version:** 1.0 (Baseline)  
> **Architecture Layer:** `global/` (System-Wide Invariants)

---

## 1. System Invariants & Business Rules

### BR-M0-01: Giới hạn tài nguyên và Phân kỳ dịch vụ (Resource Profiling)
* **Quy tắc:** Chỉ được khởi chạy nhóm dịch vụ thuộc `profile: core` (Postgres, MinIO, Polaris, Spark, Trino).
* **Ràng buộc:** Cấm kích hoạt Airflow và Superset trong giai đoạn M0. Tổng mức tiêu thụ bộ nhớ (RAM consumption) của toàn bộ container Docker không được vượt quá **6.0 GB**.

### BR-M0-02: Bền vững dữ liệu qua chu kỳ khởi động (Data Persistence)
* **Quy tắc:** Toàn bộ storage của PostgreSQL (`/var/lib/postgresql/data`) và MinIO (`/data`) bắt buộc phải sử dụng persistent docker volumes hoặc bind mounts.
* **Ràng buộc:** Lệnh `docker compose down` tuyệt đối không được làm mất dữ liệu đã seed trong Postgres và các bucket/file trên MinIO khi thực hiện `docker compose up -d` trở lại.

### BR-M0-03: Tính toàn vẹn của PII giả lập (Customer PII Integrity)
* **Quy tắc:** Bảng `customer_pii` tự sinh bằng Faker phải lấy `customer_unique_id` làm khóa chính (Primary Key).
* **Ràng buộc:** Mỗi `customer_unique_id` chỉ có đúng một bản ghi PII (họ tên, email, số điện thoại, ngày sinh). Không được sử dụng `customer_id` làm khóa vì `customer_id` bị sinh mới theo từng đơn hàng.

### BR-M0-04: Phân chia mốc thời gian của Batch (Batch Temporal Boundaries)
* **Quy tắc:** Dữ liệu lịch sử Olist phải được cắt thành 8 batch dựa vào cột `order_purchase_timestamp`:
  * **Batch B0:** Toàn bộ đơn hàng phát sinh trước ngày `2018-01-01 00:00:00`.
  * **Batch B1 đến B7:** Chia chính xác theo từng tháng từ tháng 01/2018 đến tháng 07/2018.
* **Ràng buộc:** Mỗi bản ghi trong Postgres phải được gắn thêm cột `updated_at` (mặc định bằng `order_purchase_timestamp`) để sẵn sàng cho kỹ thuật CDC/Watermark sau này.

### BR-M0-05: Tính đồng thuận của Catalog (Catalog Consensus)
* **Quy tắc:** Mọi thao tác tạo namespace, tạo bảng hoặc commit snapshot mới của Apache Iceberg bắt buộc phải đi qua Apache Polaris REST Catalog (hoặc JDBC fallback).
* **Ràng buộc:** Trino và Spark phải trỏ chung một endpoint catalog (`lakehouse`). Tuyệt đối không để Spark và Trino ghi/đọc độc lập qua các catalog metadata riêng biệt.

---
*Made by Anh Tu - Share to be share*
