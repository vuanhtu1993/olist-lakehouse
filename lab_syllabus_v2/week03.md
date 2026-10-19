# Tuần 3 (02/11 – 08/11) · Silver theory (1) & Lab M2: Bronze pipeline

**Mục tiêu:** 10 bảng nguồn vào Bronze đúng chuẩn; xử lý được batch B0, B2 (schema drift), B3 (file hỏng); Bronze không còn PII dạng raw.

## 1. Lý thuyết (~5h)
Học nửa đầu nhóm C (#21–25, Silver) theo sheet Theory_Syllabus: type casting, cleansing, business key vs surrogate key. Ôn lại #13, #16, #17, #19 vì dùng trực tiếp trong lab.

## 2. Lab M2 (~8h)
**Input:** Postgres có B0; CSV batch trong `landing/`; thiết kế tuần 2.
**Output:** `spark_jobs/bronze/` + 10 bảng `lakehouse.bronze.*`.

| Bước | Việc làm | Gợi ý code | Kiểm tra đạt |
|---|---|---|---|
| 1 | Viết `common/metadata.py`: hàm thêm 5 cột metadata | `withColumn("_ingest_timestamp", current_timestamp())`, `_batch_id` truyền vào | Mọi bảng Bronze có đủ 5 cột |
| 2 | Viết `common/hashing.py`: `record_hash(df, cols)` | chuẩn hóa: `trim`, null → `''`, sắp xếp tên cột, `concat_ws('\|')` rồi `sha2(…,256)` | Unit test: cùng dữ liệu, khác thứ tự cột → cùng hash |
| 3 | Viết `common/pii.py`: HMAC-SHA256 | UDF dùng `hmac.new(key, value, sha256)`; key lấy từ biến môi trường / Airflow Variable | Unit test: cùng email → cùng token; không có key thì không tính được |
| 4 | Job ingest Postgres (bảng 1–5) qua JDBC | đọc tất cả cột dạng string; incremental theo watermark lưu ở bảng `meta.watermark` | Chạy B0 → count Bronze = count Postgres |
| 5 | Job ingest CSV landing (bảng 6–10) | `spark.read.option("mode","PERMISSIVE").option("columnNameOfCorruptRecord","_corrupt")` | Có `_source_file_name` đúng tên file |
| 6 | Mask PII ngay khi ingest `customer_pii` | áp dụng `pii.py` cho full_name, email, phone; birth_date giữ năm | `SELECT … WHERE email LIKE '%@%'` trả 0 dòng |
| 7 | Dedup trong Bronze | bỏ dòng có cùng business key + cùng `_record_hash` đã tồn tại | Chạy lại B0 lần 2 → không tăng dòng |
| 8 | **B2 – schema drift**: nạp batch có `sales_channel` | so schema mới với schema bảng; cột mới → `ALTER TABLE ADD COLUMN` (Iceberg hỗ trợ) + ghi log `meta.schema_change` | Batch không bị reject; có 1 dòng log cảnh báo |
| 9 | **B3 – quarantine**: file reviews CSV hỏng | file không parse được → chuyển sang `landing/_quarantine/` + log | File hỏng không vào Bronze; các file khác vẫn vào |
| 10 | Thử time travel | Trino: `SELECT * FROM "oms_orders$snapshots"` | Thấy snapshot của B0 và B2 |

**Gate G3 (tự chấm):** 0 raw PII · rerun không nhân đôi · B2, B3 có bằng chứng (log + query) · unit test pass.

## 3. SQL hằng ngày (chạy bằng Trino trên Bronze)
1. Số dòng theo `_batch_id` cho từng bảng.
2. Tìm business key có > 1 version trong `crm_customers`.
3. Dedup bằng `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingest_timestamp DESC)`.
4. Query time travel: `FOR VERSION AS OF <snapshot_id>` so số dòng trước/sau B2.
5. Đếm bản ghi có `_corrupt` khác null.

## 4. Câu hỏi interview + ý chính
**Q1. Vì sao Bronze không cast kiểu dữ liệu?**
- Cast có thể làm mất dữ liệu (ngày sai định dạng thành null). Bronze là bản ghi gốc để luôn rebuild được. Cast làm ở Silver, dòng lỗi được giữ lại với lý do.

**Q2. Bạn phát hiện thay đổi bản ghi thế nào?**
- So `_record_hash` thay vì so từng cột. Hash tính trên cột business đã chuẩn hóa (trim, null, thứ tự cột), loại cột metadata ra. Cùng key, khác hash = có thay đổi → tạo version mới.

**Q3. Nguồn thêm cột mới, pipeline của bạn xử lý ra sao?**
- Không reject batch. Phát hiện cột mới, thêm vào bảng Iceberg (schema evolution không cần ghi lại dữ liệu cũ), ghi log, gửi cảnh báo để DA/DE quyết định có đưa lên Silver hay không. Cột bị mất thì để null và cảnh báo mức cao hơn.

**Q4. Khi nào quarantine ở Bronze, khi nào ở Silver?**
- Bronze chỉ cách ly thứ **không đọc được** (file hỏng, JSON sai). Dữ liệu đọc được nhưng sai nghiệp vụ (payment âm) vẫn vào Bronze; Silver mới kiểm tra DQ và cách ly.

**Q5. Incremental load theo watermark có rủi ro gì?**
- Bản ghi đến trễ có timestamp cũ hơn watermark sẽ bị bỏ sót → dùng watermark lùi lại một khoảng (lookback) + dedup; bản ghi bị sửa mà không đổi timestamp thì không bắt được → cần CDC hoặc cột `updated_at`.

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
