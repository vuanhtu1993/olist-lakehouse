# Tuần 2 (26/10 – 01/11) · Bronze theory & Lab M1: Phân tích dữ liệu và thiết kế

**Mục tiêu:** Hiểu dữ liệu Olist thật kỹ và có bộ tài liệu thiết kế trước khi viết pipeline. Đây là bước phân biệt DE "biết làm" với DE "biết nghĩ".

## 1. Lý thuyết (~6h)
| # | Khái niệm | Hiểu tới mức nào | Status |
|---|---|---|---|
| 11 | Ingest as-is 1:1 | Không cast, không đổi tên, không lọc; ngoại lệ duy nhất là mask PII | ⬜ |
| 12 | Metadata columns | `_ingest_timestamp, _source_file_name, _batch_id, _record_hash, _is_current` | ⬜ |
| 13 | Record hash SHA-256 | Hash trên cột business đã chuẩn hóa để phát hiện thay đổi | ⬜ |
| 14 | Change detection & dedup | Cùng key + cùng hash = trùng | ⬜ |
| 15 | Append-only & CDC | Giữ mọi version | ⬜ |
| 16 | Schema drift | Cột mới → ghi nhận + cảnh báo, không reject | ⬜ |
| 17 | Structural quarantine | Chỉ cách ly file không đọc được | ⬜ |
| 18 | Load patterns | Full / incremental / CDC / full-reload | ⬜ |
| 19 | PII masking (HMAC-SHA256) | Vì sao không dùng SHA-256 trần cho email | ⬜ |
| 20 | (theo Theory_Syllabus) | | ⬜ |
| 53 | Requirement → User story → BD/DD | | ⬜ |
| 54 | EDA / profiling | | ⬜ |

## 2. Lab M1 (~7h)
**Input:** Dữ liệu Olist trong Postgres (tuần 1).
**Output:** Thư mục `docs/` gồm 5 tài liệu.

| Bước | Việc làm | Sản phẩm | Kiểm tra đạt |
|---|---|---|---|
| 1 | Viết requirement cho 3 câu hỏi kinh doanh (doanh thu, LTV, RFM) dạng user story | `docs/requirements.md` | Mỗi story có acceptance criteria đo được |
| 2 | EDA từng bảng: số dòng, % null, distinct, min/max, phân phối | `notebooks/eda.ipynb` | Có bảng profiling cho 10 bảng |
| 3 | EDA quan hệ: order_items có product_id không tồn tại? order không có payment? | `docs/eda_report.md` | Liệt kê ≥ 5 phát hiện, mỗi phát hiện có số liệu |
| 4 | Chọn load pattern + business key cho 10 bảng | bảng trong `docs/design.md` | Khớp mục 2 file overview, có lý do |
| 5 | Vẽ data model Gold (star schema) | `docs/data_model.md` (Mermaid) | Ghi rõ grain của fact |
| 6 | Viết STTM Bronze → Silver → Gold | `docs/sttm.xlsx` | Mỗi cột đích có: nguồn, rule, kiểu dữ liệu, Sensitivity |
| 7 | DQ rule catalog | `docs/dq_catalog.md` | Mỗi rule có severity (Critical/Major/Minor) + ngưỡng dựa trên EDA |

**Ví dụ dòng STTM:**
| Tầng | Bảng đích | Cột đích | Nguồn | Rule | Sensitivity |
|---|---|---|---|---|---|
| Silver | customer | email_token | bronze.crm_customer_pii.email | Đã HMAC ở Bronze, chỉ verify không còn raw | PII-High |
| Gold | fact_order_item | customer_sk | silver.orders.customer_id → dim_customer | Lookup SK theo `_valid_from/_valid_to`, không thấy → -1 | Internal |

**Ví dụ DQ rule:** `payment_value >= 0` · severity Major · vi phạm > 1% batch → dừng pipeline, ≤ 1% → quarantine dòng.

## 3. SQL hằng ngày
1. % null của từng cột trong `orders` (một query dùng `COUNT(*) - COUNT(col)`).
2. Tìm order_items có product_id không có trong products (`LEFT JOIN ... WHERE p.product_id IS NULL`).
3. Đơn có nhiều hơn 1 payment.
4. Thời gian giao hàng trung bình theo bang (dùng CTE).
5. Đơn bị giao trễ so với `order_estimated_delivery_date`, tỷ lệ theo tháng.

## 4. Câu hỏi interview + ý chính
**Q1. Grain của fact table là gì? Bạn chọn grain nào cho Olist và vì sao?**
- Grain = một dòng trong fact đại diện cho điều gì. Phải chốt trước khi chọn cột.
- Chọn "1 sản phẩm trong 1 đơn" (order item) vì đây là mức chi tiết nhất có giá; từ đó cộng lên được doanh thu theo đơn, theo sản phẩm, theo người bán. Nếu chọn grain đơn hàng thì mất khả năng phân tích theo sản phẩm.

**Q2. Vì sao phải làm EDA trước khi viết DQ rule?**
- Ngưỡng DQ phải dựa trên thực tế dữ liệu. Ví dụ EDA cho thấy ~1.4% sản phẩm thiếu category (hãy dùng số bạn đo được) → nếu đặt rule "không được null" severity Critical thì pipeline dừng mãi. Phải chọn: map về Unknown và theo dõi tỷ lệ.

**Q3. STTM dùng để làm gì?**
- Là "hợp đồng" giữa DA/business và DE: cột đích lấy từ đâu, biến đổi gì, kiểm tra gì, nhạy cảm mức nào. Dùng để review, viết test và truy vết lineage.

**Q4. Phân biệt `customer_id` và `customer_unique_id` trong Olist. Ảnh hưởng gì đến thiết kế?**
- `customer_id` sinh theo từng đơn; `customer_unique_id` là một người. Tính LTV/RFM phải theo `customer_unique_id`, nếu không mỗi khách sẽ bị đếm nhiều lần và LTV bị thấp sai.

**Q5. Vì sao mask PII bằng HMAC chứ không dùng SHA-256?**
- Email/SĐT có không gian giá trị nhỏ, kẻ xấu có thể hash thử danh sách email để dò ngược. HMAC cần secret key nên không dò được, nhưng vẫn deterministic → cùng email ra cùng token, vẫn join và dedup được.

## 5. Nhật ký tuần
| Đã làm | Vướng | Điều chỉnh |
|---|---|---|
| | | |
