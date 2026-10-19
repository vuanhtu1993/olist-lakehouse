# Tuần 1 (19/10 – 25/10) · Foundations & Lab M0: Dựng môi trường + nạp dữ liệu nguồn

**Mục tiêu:** Kết thúc tuần, máy bạn chạy được toàn bộ stack và có dữ liệu Olist nằm trong Postgres, query thử được 1 bảng qua Trino.
**Bối cảnh project:** Xem [00_PROJECT_OVERVIEW.md](00_PROJECT_OVERVIEW.md) mục 2 và 4.  
**Kế hoạch thực thi chi tiết & phân rã nhiệm vụ:** Xem [2026-10-19-week01-foundations-lab-m0.md](../plan/2026-10-19-week01-foundations-lab-m0.md).

## 1. Lý thuyết (~6h)
| # | Khái niệm | Hiểu tới mức nào | Status |
|---|---|---|---|
| 1 | Data Lakehouse | Vì sao kết hợp lake + warehouse; storage tách compute | ⬜ |
| 2 | Medallion | Bronze/Silver/Gold mỗi tầng làm gì, ai dùng | ⬜ |
| 3 | ETL vs ELT | Vì sao Bronze nạp thô trước | ⬜ |
| 4 | Parquet vs CSV | Columnar, nén, predicate pushdown | ⬜ |
| 5 | Apache Iceberg | Snapshot, time travel, schema evolution, ACID | ⬜ |
| 6 | Catalog (Polaris) | Catalog lưu con trỏ tới metadata mới nhất | ⬜ |
| 7 | Spark vs Trino | Spark = biến đổi batch; Trino = query tương tác | ⬜ |
| 8 | Airflow / DAG | Task, dependency, schedule, retry | ⬜ |
| 9 | tenant_id, RLS | Cô lập dữ liệu nhiều khách hàng trong cùng bảng | ⬜ |
| 10 | STTM | Bảng mapping nguồn → đích | ⬜ |
| 50–52 | Docker Compose, Postgres, Git | Đủ dùng để làm lab | ⬜ |

## 2. Lab M0 (~7h)
**Input:** Máy có Docker (≥ 8GB RAM cho Docker), tài khoản Kaggle.
**Output:** Repo `olist-lakehouse` với stack chạy được và Postgres có dữ liệu.

| Bước | Việc làm | Lệnh / file gợi ý | Kiểm tra đạt |
|---|---|---|---|
| 1 | Tạo repo theo cấu trúc mục 6 file overview | `git init`, tạo thư mục | Push lên GitHub |
| 2 | Postgres + MinIO trong `docker-compose.yml` | image `postgres:16`, `minio/minio` | Mở MinIO console :9001, tạo 4 bucket `landing, bronze, silver, gold` |
| 3 | Thêm Polaris (REST catalog) + Trino | `etc/catalog/lakehouse.properties` với `connector.name=iceberg`, `iceberg.catalog.type=rest` | `SHOW CATALOGS` trong Trino thấy `lakehouse` |
| 4 | Thêm Spark (hoặc Jupyter PySpark) có Iceberg runtime | cấu hình `spark.sql.catalog.lakehouse` trỏ Polaris | Spark tạo được namespace `bronze` |
| 5 | Thêm Airflow + Superset | `apache/airflow`, `apache/superset` | UI Airflow :8080, Superset :8088 mở được |
| 6 | Tải dữ liệu | `kaggle datasets download olistbr/brazilian-ecommerce` → `data_prep/raw/` | Có 9 file CSV |
| 7 | Sinh PII giả | `data_prep/gen_pii.py`: Faker sinh full_name, email, phone, birth_date theo `customer_unique_id` | `customer_pii.csv` có số dòng = số `customer_unique_id` |
| 8 | Thêm `updated_at` và cắt batch | `data_prep/split_batches.py`: chia theo tháng ra `batch_B0 … B7` | Có 8 thư mục batch |
| 9 | Seed Postgres DB `oltp_olist` với batch B0 (bảng 1–5) | `data_prep/seed_postgres.py` (pandas + SQLAlchemy) | `SELECT count(*) FROM orders` > 0 |
| 10 | Hello world | Spark đọc `orders` qua JDBC → ghi `lakehouse.bronze.oms_orders_hello` | Trino: `SELECT count(*) FROM lakehouse.bronze.oms_orders_hello` ra đúng số |

**Gate G1 (tự chấm):** `docker compose down && up` xong dữ liệu vẫn còn · Trino query được bảng Iceberg · commit đầy đủ.

## 3. SQL hằng ngày (30 phút, chạy trên Postgres tuần này)
1. Đếm số đơn theo `order_status`.
2. Top 5 bang có nhiều khách nhất.
3. Số `customer_id` vs số `customer_unique_id` khác nhau bao nhiêu? Vì sao?
4. Đơn mới nhất của mỗi `customer_unique_id` (dùng `ROW_NUMBER()`).
5. Doanh thu (`price + freight_value`) theo tháng.

## 4. Câu hỏi interview + ý chính cần trả lời
**Q1. Data Lakehouse khác Data Lake và Data Warehouse thế nào?**
- Lake: file rẻ, linh hoạt nhưng không có ACID, dễ thành "data swamp".
- Warehouse: ACID, schema, SQL nhanh nhưng đắt, khó chứa dữ liệu bán cấu trúc, lưu trữ gắn với compute.
- Lakehouse: lưu file mở (Parquet) trên object storage + table format (Iceberg/Delta) để có ACID, schema evolution, time travel. Nhiều engine (Spark, Trino) đọc chung một bản dữ liệu.
- *Gắn project:* "Trong project, tôi lưu Parquet trên MinIO, Iceberg quản lý bảng, Spark ghi và Trino đọc cùng một bảng."

**Q2. Vì sao cần Catalog khi đã có Iceberg?**
- Iceberg chỉ là đặc tả + thư viện; mỗi lần commit tạo file metadata mới.
- Catalog giữ con trỏ "bảng X → metadata mới nhất", và việc đổi con trỏ là atomic → đảm bảo ACID khi nhiều engine ghi.
- Không có catalog, Trino không biết bảng ở đâu.

**Q3. Khi nào dùng Spark, khi nào dùng Trino?**
- Spark: biến đổi khối lượng lớn, job dài, cần chịu lỗi, logic phức tạp bằng Python.
- Trino: query SQL tương tác, đối soát, phục vụ BI, độ trễ thấp.
- Không nên dùng Trino cho ETL nặng chạy hàng giờ (không có checkpoint chịu lỗi như Spark).

**Q4. Parquet tốt hơn CSV cho analytics vì sao?**
- Lưu theo cột → chỉ đọc cột cần; nén tốt hơn; có schema và kiểu dữ liệu; có thống kê min/max để bỏ qua block (predicate pushdown).

**Q5 (behavioral). Bạn là Software Engineer, kinh nghiệm nào dùng được cho Data Engineering?**
- Docker, Git, code review, viết test, thiết kế module, debug. Ví dụ: "Tôi tổ chức project thành `common/` dùng chung, có unit test cho hàm hash ngay từ tuần đầu."

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
