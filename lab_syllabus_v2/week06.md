# Tuần 6 (23/11 – 29/11) · Cross-layer theory & Lab M5: Orchestration, idempotency, backfill

**Mục tiêu:** Một DAG Airflow chạy B → S → G tự động, chạy lại an toàn (B6), backfill được (B7), có lineage và SLA.

## 1. Lý thuyết (~5h)
Nhóm E (#40–48) theo Theory_Syllabus: audit & lineage, error handling, idempotent reprocessing, backfill, gating, SLA, #46 naming convention.

## 2. Lab M5 (~8h)
**Input:** Job Bronze/Silver/Gold đã chạy tay được. **Output:** `dags/olist_pipeline.py`, `docs/runbook_backfill.md`.

| Bước | Việc làm | Gợi ý | Kiểm tra đạt |
|---|---|---|---|
| 1 | DAG 3 nhóm task: bronze → silver → gold | `SparkSubmitOperator` hoặc `BashOperator`; tham số `batch_id`, `logical_date` | DAG chạy xanh với B0 |
| 2 | Gating DQ | task `dq_gate` đọc `meta.dq_result`; Critical fail → `AirflowFailException` | Cố ý cài lỗi Critical → Gold không chạy |
| 3 | Retry + alert | `retries=2`, `retry_delay`, `on_failure_callback` ghi log / gửi email | Kill 1 task → tự retry |
| 4 | Bảng audit/lineage | mỗi task ghi `run_id, batch_id, source_table, target_table, rows_in, rows_out, status, duration` | Truy được bảng Gold sinh từ batch nào |
| 5 | **B6 – idempotency** | ghi bằng `MERGE` hoặc overwrite theo partition, không `append` mù | Chạy B6 (trùng B5) → số dòng Gold không đổi |
| 6 | **B7 – backfill** 2018-03 | viết runbook 9 bước: thông báo → đóng băng → nạp lại Bronze → rebuild Silver/Gold partition 2018-03 → đối soát → mở lại; `airflow dags backfill -s 2018-03-01 -e 2018-03-31` | Chỉ partition 2018-03 thay đổi (so snapshot) |
| 7 | SLA | `sla=timedelta(...)` hoặc task kiểm tra độ trễ | Có cảnh báo khi vượt SLA |
| 8 | Đặt tên lại theo convention #46 | | Tên bucket/namespace/bảng thống nhất |

**Gate G6:** Pipeline tự động · B6 không trùng · B7 thành công có bằng chứng · lineage truy được.

## 3. SQL + System design
- SQL: tìm dòng trùng sau rerun (`GROUP BY … HAVING COUNT(*)>1`); so 2 snapshot Iceberg trước/sau backfill; tính thời gian chạy trung bình mỗi task từ bảng audit.
- **System design #1 (1 buổi cuối tuần):** "Thiết kế pipeline đồng bộ dữ liệu đơn hàng gần thời gian thực từ Postgres." Ý cần có: CDC bằng Debezium/Kafka hoặc watermark, Bronze append, dedup, SCD, DQ gate, orchestration, monitoring, cách backfill.

## 4. Câu hỏi interview + ý chính
**Q1. Idempotent pipeline là gì? Bạn làm thế nào?**
- Chạy lại cùng input nhiều lần cho cùng kết quả. Cách: MERGE theo key hoặc overwrite theo partition; dedup theo hash; không dùng `now()` trong logic nghiệp vụ mà dùng `logical_date`.

**Q2. Mô tả quy trình backfill an toàn.**
- Xác định phạm vi → thông báo người dùng → chạy theo partition → đối soát trước/sau → có thể rollback nhờ Iceberg snapshot → ghi lại trong runbook.

**Q3. Task giữa chừng bị lỗi thì sao?**
- Nhờ idempotent nên chỉ cần chạy lại task đó; Iceberg commit atomic nên không có dữ liệu ghi dở; gate ngăn lỗi lan xuống tầng dưới.

**Q4. Lineage để làm gì?**
- Khi số dashboard sai, truy ngược bảng → batch → file nguồn; đánh giá ảnh hưởng khi đổi một cột nguồn.

**Q5. Airflow có nên xử lý dữ liệu không?**
- Không. Airflow chỉ điều phối; xử lý nặng giao cho Spark/Trino. Worker Airflow xử lý dữ liệu lớn sẽ quá tải và khó mở rộng.

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
