# Tuần 7 (30/11 – 06/12) · Serving & Testing · Lab M6 (Dashboard) + M7 (Test)

**Mục tiêu:** Dashboard Superset có số liệu khớp với Gold, bộ test tự động và bằng chứng E2E. Bắt đầu chuẩn bị câu chuyện cá nhân cho interview.

## 1. Lý thuyết (~4.5h)
| # | Khái niệm | Status |
|---|---|---|
| 55 | BI serving với Superset | ⬜ |
| 56 | Unit test & data compare | ⬜ |
| 57 | SIT/E2E vs UAT, test evidence | ⬜ |
| 58 | Release, runbook, hypercare | ⬜ |
| 59 | **AI-Ready Data Pipelines:** Xử lý text phi cấu trúc, sentiment enrichment & serving context cho LLM/Analytics | ⬜ |

## 2. Lab M6 + M7 + AI Extension (~9.5h)
| Bước | Việc làm | Gợi ý | Kiểm tra đạt |
|---|---|---|---|
| 1 | Superset kết nối Trino | URI `trino://user@trino:8080/lakehouse` | Thấy schema `gold` |
| 2 | Dataset + chart | doanh thu theo tháng (line), theo bang (bar/map), RFM segment (pie), top sản phẩm (table) | 4 chart trên 1 dashboard |
| 3 | Serving SQL | dùng bảng KPI, không query fact thô cho chart tổng | Dashboard load < vài giây |
| 4 | Tie-out | 3 con số trên dashboard so với query Trino trực tiếp | Khớp 100%, chụp màn hình vào `dashboard/` |
| 5 | **AI-Enriched Pipeline (M7-AI)** | Viết mini-job Spark/Python trích xuất sentiment & phân loại lý do tiêu cực (chậm hàng, hỏng, sai mô tả) từ `olist_order_reviews_dataset` qua model NLP/LLM offline (HuggingFace Transformers) hoặc API | Bảng `gold.fact_review_sentiment` có cột `sentiment_score`, `issue_category` |
| 6 | Trực quan hóa AI Insight | Thêm chart tỷ lệ hài lòng theo bang/danh mục và Top lý do phàn nàn trên Superset | Dashboard có góc nhìn AI Analytics |
| 7 | Unit test (pytest + Spark local) | test `record_hash`, `hmac_token`, cast, SCD2 trên DataFrame nhỏ | `pytest` pass |
| 8 | Data compare | script so count/sum giữa các tầng sau mỗi batch | Chạy tự động trong DAG |
| 9 | Test E2E | thả 1 batch mới → DAG → dashboard cập nhật | Biên bản E2E có ảnh trước/sau |
| 10 | CI đơn giản | GitHub Actions chạy `pytest` khi push | Badge xanh trên README |

## 3. SQL + System design
- SQL: viết lại 5 bài khó nhất các tuần trước trong 30 phút, không xem đáp án.
- **System design #2:** "Dashboard chậm khi người dùng tăng 10 lần – bạn làm gì?" (bảng aggregate, partition/sort, cache Superset, tách cluster Trino, compaction).
- **System design #3 (AI-Ready):** "Thiết kế pipeline xử lý dữ liệu phi cấu trúc (Reviews, PDF, ảnh sản phẩm) để cấp cho hệ thống RAG/LLM Search của doanh nghiệp."

## 4. Câu chuyện cá nhân (STAR) – viết bằng tiếng Anh
**"Why move from Software Engineering to Data Engineering in the AI Era?"**
- *Situation:* Là SE, nhận thấy 80% sức mạnh của mô hình AI và quyết định kinh doanh phụ thuộc vào chất lượng dữ liệu nền tảng (Garbage in, Garbage out).
- *Task:* Muốn trở thành DE am hiểu cả Data Platform truyền thống lẫn luồng dữ liệu cho AI (AI-ready Data Pipelines).
- *Action:* Tự xây Medallion Lakehouse trên Iceberg/Spark/Trino, kiểm soát chất lượng qua Fault-Injection (B0–B7), tích hợp module NLP phân loại review khách hàng tự động vào Gold layer.
- *Result:* Pipeline tự động hóa 100%, có bằng chứng tie-out và test coverage; mang tư duy clean code, unit test, CI/CD của SE sang Data.

## 5. Câu hỏi interview + ý chính
**Q1. Bạn test một data pipeline thế nào?**
- 3 lớp: unit test cho hàm biến đổi; data test (DQ rule, reconcile giữa tầng) chạy mỗi batch; E2E với dữ liệu cài lỗi có chủ đích. Thêm CI chạy test khi push.

**Q2. AI làm thay đổi công việc của Data Engineer như thế nào?**
- AI tự động sinh code cơ bản, nhưng DE chịu trách nhiệm về Data Quality, Lineage, Governance, và chuẩn bị Ground Truth context cho LLM/RAG (như pipeline trích xuất sentiment từ review ở tầng Gold).

**Q3. Dashboard hiển thị sai số, bạn debug thế nào?**
- Tie-out ngược: dashboard → bảng KPI → fact → Silver → Bronze → nguồn; dùng lineage và snapshot để biết thay đổi ở batch nào.

**Q4. Vì sao không cho dashboard query thẳng bảng fact lớn?**
- Chậm, tốn tài nguyên, logic KPI bị lặp ở nhiều chart. Dùng bảng KPI/aggregate làm lớp serving thống nhất.

## 6. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
