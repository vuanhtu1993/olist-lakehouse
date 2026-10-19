# Tuần 5 (16/11 – 22/11) · Gold theory & Lab M4: Star schema + KPI

**Mục tiêu:** Gold có star schema, 3 bảng KPI trả lời đúng 3 câu hỏi kinh doanh, số liệu khớp với Silver.

## 1. Lý thuyết (~5h)
Nhóm D (#31–39) theo Theory_Syllabus: conformed dimension, surrogate key, fact với SK resolution, aggregate, KPI, #37 tối ưu hiệu năng Iceberg (partition spec, sort order, compaction).

## 2. Lab M4 (~8h)
**Input:** Silver (tuần 4). **Output:** `spark_jobs/gold/` + bảng `lakehouse.gold.*` + `docs/reconcile_report.md`.

| Bước | Việc làm | Gợi ý | Kiểm tra đạt |
|---|---|---|---|
| 1 | `dim_date` | sinh từ 2016-01-01 đến 2018-12-31, `date_sk = yyyymmdd` | Đủ ngày, không trùng |
| 2 | `dim_product`, `dim_seller` | SK tự sinh; thêm dòng Unknown SK = -1 | Có đúng 1 dòng -1 mỗi dim |
| 3 | `dim_customer` từ SCD2 Silver | giữ `_valid_from/_valid_to`; thêm `age_band` từ năm sinh | Số dòng current = số `customer_unique_id` |
| 4 | `fact_order_item` | join Silver order_items + orders; lookup SK; customer SK chọn theo thời điểm mua nằm trong khoảng hiệu lực | Không có SK null (thiếu → -1) |
| 5 | Partition fact | `PARTITIONED BY (months(order_purchase_ts))`, sort theo `customer_sk` | Query 1 tháng chỉ quét 1 partition (xem `EXPLAIN`) |
| 6 | `kpi_monthly_revenue` | doanh thu theo tháng × bang × category | |
| 7 | `kpi_customer_ltv` | tổng chi tiêu, số đơn, AOV theo `customer_unique_id` | |
| 8 | `kpi_customer_rfm` | R = số ngày từ đơn cuối, F = số đơn, M = tổng tiền; `NTILE(5)` mỗi chiều; gán segment (Champions, At Risk…) | Mỗi khách có 1 segment |
| 9 | Đối soát Gold ↔ Silver | tổng price, số order item, số đơn | Chênh lệch = 0 hoặc giải thích được |

**Gate G5:** Star schema query được · 3 KPI · reconcile report.

## 3. SQL hằng ngày
1. Doanh thu theo tháng + running total (`SUM() OVER (ORDER BY month)`).
2. Tăng trưởng MoM (`LAG`).
3. Top 3 sản phẩm mỗi category (`DENSE_RANK`).
4. RFM bằng `NTILE(5)`.
5. Cohort: khách theo tháng mua đầu tiên, tỷ lệ quay lại tháng sau.

## 4. Câu hỏi interview + ý chính
**Q1. Star schema vs snowflake?**
- Star: dim phẳng, ít join, query nhanh, dễ cho BI. Snowflake: chuẩn hóa dim, tiết kiệm lưu trữ nhưng nhiều join. Lakehouse lưu rẻ nên thường chọn star.

**Q2. Vì sao dùng surrogate key thay vì business key?**
- Business key có thể đổi hoặc trùng giữa nguồn; SCD2 cần nhiều dòng cho một business key → cần SK phân biệt version; SK -1 cho Unknown.

**Q3. Late-arriving dimension là gì? Xử lý thế nào?**
- Fact đến trước dim (B4). Gán -1 hoặc tạo dòng dim tạm (inferred member), khi dim đến thì cập nhật. Trong project tôi dùng -1 + log + job sửa lại.

**Q4. Partition fact theo cột nào?**
- Theo cột hay lọc nhất và phân bố đều: tháng mua hàng. Không partition theo customer (quá nhiều partition nhỏ). Iceberg có hidden partitioning nên người query không cần biết cột partition.

**Q5. Bạn chứng minh số liệu Gold đúng thế nào?**
- Reconcile theo tầng (count, sum) + tie-out một vài con số với query trực tiếp từ nguồn + test với case cố ý cài lỗi (B4, B5).

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
