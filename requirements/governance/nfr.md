# Governance 01 — Non-Functional Requirements (NFR): Olist-Lakehouse-M0

> **Parent Document:** [Requirement Analysis Package](../README.md)  
> **Version:** 1.0 (Baseline)  
> **Architecture Layer:** `governance/` (Quality Control & Architecture)

---

## 1. NFR Metrics & Standards (10 Metrics for M0)

| ID | Metric / Quality Attribute | Target Standard | Verification Method |
|---|---|---|---|
| **NFR-M0-01** | **Memory Consumption** | Toàn bộ cụm core containers tiêu thụ **< 6.0 GiB RAM** | Đo qua `docker stats --no-stream` |
| **NFR-M0-02** | **Storage Persistence** | 0% mất mát dữ liệu sau chu kỳ `docker compose down && up` | Query count Postgres & MinIO bucket check |
| **NFR-M0-03** | **Execution Performance** | Smoke test pipeline (Postgres -> Spark -> MinIO -> Trino) hoàn thành **< 60 giây** | Đo thời gian chạy notebook / script |
| **NFR-M0-04** | **Secret Security** | 0 secret / password / key cứng được commit vào Git repo | Quét repo bằng git-secrets hoặc regex scan |
| **NFR-M0-05** | **Reproducibility** | Người khác clone repo chỉ cần tối đa 3 lệnh để dựng lại toàn bộ stack | Thực hiện clone sang thư mục sạch và chạy thử |
| **NFR-M0-06** | **Network Isolation** | Toàn bộ giao tiếp giữa các container diễn ra trên Docker network nội bộ (`lakehouse_net`) | `docker network inspect` |
| **NFR-M0-07** | **ACID Conformance** | Bảng Iceberg tuân thủ chuẩn Snapshot Isolation (v2 table format) | Kiểm tra `oms_orders_smoke$snapshots` |
| **NFR-M0-08** | **Data Integrity** | Không có khóa ngoại mồ côi (orphan keys) khi nạp B0 vào PostgreSQL | Script SQL integrity check |
| **NFR-M0-09** | **Log Traceability** | Mọi container cấu hình logging driver dạng JSON với kích thước tối đa 20MB (`max-size: 20m`) | Cấu hình trong `docker-compose.yml` |
| **NFR-M0-10** | **Port Non-Collision** | Các cổng host (5432, 9000, 9001, 8181, 8080, 8888) không xung đột với các service máy chủ | Thiết lập qua biến `.env` có thể override |

---
*Made by Anh Tu - Share to be share*
