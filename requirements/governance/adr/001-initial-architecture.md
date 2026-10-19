# ADR 001 — Lựa chọn Table Format (Iceberg) và REST Catalog (Polaris)

> **Status:** Accepted  
> **Date:** 2026-10-19  
> **Deciders:** Anh Tú (Data Engineer & Architect)  
> **Technical Scope:** Lakehouse Storage Layer & Catalog Engine

---

## 1. Context & Problem Statement
Dự án Olist Lakehouse yêu cầu một định dạng lưu trữ hỗ trợ giao dịch ACID, cho phép cập nhật lịch sử khách hàng (SCD Type 2), xử lý Schema Drift và cho phép nhiều compute engine (Apache Spark và Trino) truy cập đồng thời. Cần chọn một Table Format mở và một Catalog Engine đóng vai trò con trỏ nguyên tử (Atomic Pointer) mà không bị phụ thuộc vào một nhà cung cấp đám mây duy nhất.

---

## 2. Decision
Chúng tôi quyết định:
1. Sử dụng **Apache Iceberg (v2 Table Format)** làm định dạng bảng chính thức trên Object Storage MinIO.
2. Sử dụng **Apache Polaris** làm **Iceberg REST Catalog** tập trung.
3. Dự phòng kỹ thuật: Nếu Polaris gặp lỗi tương thích trong quá trình dựng môi trường (vượt quá Timebox 2.5h), chuyển ngay sang **Iceberg JDBC Catalog** trên PostgreSQL theo đúng kế hoạch B.

---

## 3. Considered Options
* **Option 1:** Apache Iceberg + Polaris REST Catalog *(Được chọn)*
* **Option 2:** Delta Lake + Hive Metastore (HMS)
* **Option 3:** Raw Parquet + Hive Metastore

---

## 4. Consequences & Trade-offs (Why over What)

### Điểm tích cực (Pros):
* **Tính độc lập & Chuẩn mở:** REST Catalog là chuẩn mở hiện đại nhất của cộng đồng Iceberg, tách rời hoàn toàn tầng metadata khỏi engine tính toán.
* **Tương thích đa engine:** Cả Spark (để ghi batch) và Trino (để truy vấn tương tác) đều hỗ trợ native Iceberg REST connector mà không cần cấu hình phức tạp.
* **Hỗ trợ ACID & Schema Evolution:** Không bị lỗi corrupt file khi có sự cố mạng giữa chừng; thêm cột không cần rewrite toàn bộ bảng.

### Đánh đổi & Hạn chế (Trade-offs / Cons):
* **Độ chín muồi của công nghệ:** Polaris mới được Snowflake open-source vào giữa năm 2024, tài liệu cộng đồng còn ít hơn Hive Metastore.
* **Phương án khắc phục:** Giới hạn timebox 2.5 giờ; nếu cấu hình gặp trục trặc, kích hoạt JDBC Catalog trên Postgres ngay lập tức để không làm trễ tiến độ.

---
*Made by Anh Tu - Share to be share*
