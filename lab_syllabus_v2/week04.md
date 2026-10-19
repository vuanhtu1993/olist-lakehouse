# Tuần 4 (09/11 – 15/11) · Silver theory (2) & Lab M3: Silver pipeline

**Mục tiêu:** Tầng Silver sạch, đúng kiểu, có lịch sử khách hàng (SCD2), xử lý được B1, B4, B5 và có báo cáo DQ.

## 1. Lý thuyết (~5h)
Học hết nhóm C (#26–30) theo Theory_Syllabus: dedup ở Silver, referential integrity, SCD2, DQ severity và quarantine. Tự so sánh SCD Type 0/1/2/3 trên giấy trước khi code.

## 2. Lab M3 (~8h)
**Input:** Bronze đã có B0 (và B2, B3).
**Output:** `spark_jobs/silver/` + bảng `lakehouse.silver.*` + `docs/dq_report.md`.

| Bước | Việc làm | Gợi ý | Kiểm tra đạt |
|---|---|---|---|
| 1 | Cast kiểu theo STTM | `to_timestamp`, `cast("decimal(12,2)")`; dòng cast lỗi → bảng `silver_quarantine` có cột `_dq_reason` | Không mất dòng: Silver + quarantine = Bronze (version hiện tại) |
| 2 | Chuẩn hóa | trim, lower email token, state viết hoa, conform zip qua `ref_geolocation` | Không còn state ngoài 27 bang Brazil |
| 3 | Dedup Silver | giữ bản mới nhất theo business key | Không có key trùng |
| 4 | Viết `common/dq.py` | mỗi rule trả về số dòng vi phạm; severity Critical → dừng job, Major → quarantine dòng, Minor → chỉ log | Có bảng `meta.dq_result` theo batch |
| 5 | **B1 – SCD2** cho customer | `MERGE INTO`: khách đổi city/state → đóng dòng cũ (`_valid_to = now`, `_is_current=false`), chèn dòng mới | 1 khách đổi địa chỉ có 2 dòng, đúng 1 dòng current |
| 6 | **B4 – RI**: order_items trỏ product không tồn tại | không drop; gán `product_id = 'UNKNOWN'` (sẽ thành SK -1 ở Gold) + log | Count order_items Silver = Bronze |
| 7 | Category null → `'unknown'` | | Không còn null category |
| 8 | **B5 – DQ**: ngày sai, payment âm, email sai | áp dụng rule catalog tuần 2 | Có số liệu vi phạm theo rule trong DQ report |
| 9 | Verify PII | Silver không mask lại, chỉ kiểm tra không có raw | Rule Critical pass |

**Gate G4:** Silver đối soát được với Bronze · SCD2 đúng · DQ report có đủ B1, B4, B5.

## 3. SQL hằng ngày
1. Viết lại `MERGE INTO` SCD2 bằng tay trong Trino trên bảng nhỏ.
2. Tìm khách có > 1 version và in lịch sử địa chỉ.
3. `LAG()` để phát hiện khách đổi bang giữa 2 đơn liên tiếp.
4. **Gaps & islands:** tìm chuỗi ngày liên tiếp có đơn hàng dài nhất.
5. Query "trạng thái khách hàng tại một thời điểm": `WHERE ts BETWEEN _valid_from AND _valid_to`.

## 4. Câu hỏi interview + ý chính
**Q1. SCD1 và SCD2 khác gì? Khi nào dùng mỗi loại?**
- SCD1 ghi đè, không giữ lịch sử – dùng khi sửa lỗi chính tả, thuộc tính không cần phân tích theo thời gian.
- SCD2 tạo dòng mới với khoảng hiệu lực – dùng khi cần báo cáo đúng tại thời điểm (doanh thu theo bang tại thời điểm mua). Trong project: city/state của khách dùng SCD2.

**Q2. Order item trỏ tới sản phẩm không tồn tại, bạn làm gì?**
- Không drop vì sẽ làm sai doanh thu. Gán về thành viên Unknown (SK = -1), log lại, theo dõi tỷ lệ. Khi sản phẩm đến sau (late-arriving), có thể cập nhật lại.

**Q3. Bạn thiết kế DQ severity thế nào?**
- Critical: sai thì dừng pipeline (PII lộ, khóa chính null). Major: cách ly dòng, pipeline chạy tiếp, cảnh báo. Minor: chỉ ghi nhận. Ngưỡng theo % batch, dựa trên EDA.

**Q4. Làm sao đảm bảo không "mất dòng" khi làm sạch?**
- Đối soát: số dòng Silver + quarantine = số dòng đầu vào. Mỗi dòng bị loại có `_dq_reason`.

**Q5 (SQL live).** "Lấy đơn mới nhất của mỗi khách" → nói được 2 cách: `ROW_NUMBER()` và `JOIN` với `MAX()`; nêu trường hợp trùng timestamp.

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
