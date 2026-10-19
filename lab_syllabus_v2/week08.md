# Tuần 8 (07/12 – 13/12) · Lab M8 Go-live/Portfolio & Ôn tập interview

**Mục tiêu:** Repo hoàn chỉnh để đưa vào CV, trình bày được project trong 5 phút bằng tiếng Anh, 58/58 khái niệm passed. Interview từ 14/12.

## 1. Lab M8 (~6h)
| Bước | Việc làm | Kiểm tra đạt |
|---|---|---|
| 1 | README: bài toán, kiến trúc (Mermaid), dataset, cách chạy trong 5 lệnh, ảnh dashboard, kết quả test | Người lạ clone về chạy được |
| 2 | `docs/runbook.md`: khởi động, kiểm tra sức khỏe, xử lý lỗi thường gặp, backfill, rotate HMAC key | Đủ 5 mục |
| 3 | Bảng "Thách thức & cách giải" trong README (B1–B7) | 7 dòng, mỗi dòng có bằng chứng |
| 4 | Dọn code, xóa secret, `.env.example` | Không lộ key trên GitHub |
| 5 | Cập nhật CV: 3–4 bullet đo được | Ví dụ bên dưới |

**Mẫu bullet CV:**
- Built an end-to-end Medallion Lakehouse (MinIO, Iceberg, Polaris, Spark, Trino, Airflow, Superset) on the Olist dataset (~100k orders).
- Implemented HMAC-SHA256 PII tokenization at ingestion, SCD2 customer history and idempotent MERGE-based loads.
- Designed DQ gating with severity levels and layer reconciliation; replayed 8 batches with injected faults (schema drift, orphan keys, duplicates, backfill).
- Added pytest unit tests and GitHub Actions CI.

## 2. Ôn lý thuyết (~5h)
| Ngày | Ôn | Cách ôn |
|---|---|---|
| T2 08/12 | Nhóm A–B | Tự nói 2 phút/khái niệm bằng tiếng Anh, ghi âm nghe lại |
| T3 09/12 | Nhóm C | Vẽ lại luồng SCD2 không xem tài liệu |
| T4 10/12 | Nhóm D | Vẽ star schema + giải thích grain |
| T5 11/12 | Nhóm E–F | Kể lại quy trình backfill |
| T6 12/12 | Mock interview lý thuyết | Nhờ đồng nghiệp hoặc AI đóng vai interviewer |
| T7 13/12 | Mock SQL + demo project 10 phút | Bấm giờ |

## 3. Bộ câu hỏi tổng hợp (luyện trả lời)
**Project deep-dive**
1. Walk me through your project. *(Bài toán → kiến trúc → 3 điểm kỹ thuật khó → kết quả, tối đa 5 phút)*
2. Điểm khó nhất là gì? *(Gợi ý: SCD2 + lookup SK theo thời điểm, hoặc idempotent backfill)*
3. Nếu làm lại, bạn thay đổi gì? *(dbt cho transform SQL, Great Expectations cho DQ, streaming CDC)*
4. Dữ liệu tăng 100 lần thì sao? *(partition/compaction, tăng worker Spark, incremental thay full, tách cluster Trino, tối ưu file size)*

**Kiến thức nền**
5. ACID trên object storage hoạt động thế nào với Iceberg?
6. Batch vs streaming, khi nào cần streaming?
7. Data skew trong Spark là gì, xử lý thế nào? *(salting, broadcast join, AQE)*
8. OLTP vs OLAP.
9. Normalization vs denormalization.

**SQL live** – window function, CTE, dedup, gaps & islands, top-N mỗi nhóm, running total.

**Behavioral** – 3 câu STAR đã chuẩn bị tuần 7.

## 4. Checklist sẵn sàng interview
- [ ] Repo public, README hoàn chỉnh
- [ ] Demo chạy được trên máy (đã thử sau khi `docker compose down`)
- [ ] 58/58 khái niệm Q&A passed
- [ ] 3 câu STAR bằng tiếng Anh
- [ ] CV cập nhật

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
