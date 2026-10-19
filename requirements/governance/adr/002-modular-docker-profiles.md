# ADR 002 — Phân kỳ tài nguyên bằng Docker Compose Profiles

> **Status:** Accepted  
> **Date:** 2026-10-19  
> **Deciders:** Anh Tú (Data Engineer & Architect)  
> **Technical Scope:** Local Container Infrastructure & Memory Management

---

## 1. Context & Problem Statement
Chạy đồng thời toàn bộ 7 dịch vụ (PostgreSQL, MinIO, Polaris, Spark, Trino, Airflow, Superset) trên máy tính cá nhân đòi hỏi tối thiểu 12GB - 16GB RAM riêng cho Docker. Điều này dẫn đến nguy cơ cao bị nghẽn tài nguyên (Local Resource Crunch), tràn bộ nhớ (OOMKilled) và làm người học bị ngợp kiến thức ngay từ tuần đầu tiên.

---

## 2. Decision
Chúng tôi quyết định áp dụng nguyên tắc **Progressive Disclosure** cho hạ tầng:
1. Sử dụng tính năng **Docker Compose Profiles** để phân rã dịch vụ thành 3 nhóm độc lập:
   * Profile `core` (Tuần 1–5): Chỉ chạy `postgres`, `minio`, `polaris`, `spark`, `trino` (Tổng RAM < 6GB).
   * Profile `orchestration` (Tuần 6): Kích hoạt thêm `airflow`.
   * Profile `bi` (Tuần 7): Kích hoạt thêm `superset`.
2. Lệnh khởi động tuần 1: `docker compose --profile core up -d`.

---

## 3. Consequences & Trade-offs (Why over What)

### Điểm tích cực (Pros):
* **Bảo vệ phần cứng:** Giữ mức tiêu thụ RAM luôn dưới 6GB, máy host không bị swap giật lag.
* **Lợi ích sư phạm:** Người học tập trung hoàn toàn vào việc hiểu luồng dữ liệu (Storage, Catalog, Compute) mà không bị phân tâm bởi việc cấu hình Airflow/DAG khi chưa cần tới.

### Đánh đổi (Cons):
* Phải cấu hình các cờ profile trong `docker-compose.yml` và cần nhớ thêm tham số `--profile core` khi gõ lệnh.

---
*Made by Anh Tu - Share to be share*
