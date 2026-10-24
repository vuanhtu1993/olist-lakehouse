# 5 Câu Hỏi Phỏng Vấn Chuyên Sâu & Phản Xạ Kỹ Thuật Tuần 02 (Interview Cheatsheet)

> **Mô-đun:** Tuần 02 — Lab M1: Phân Tích Dữ Liệu & Thiết Kế Kiến Trúc Lakehouse  
> **Tác giả:** Anh Tú (Pedagogical Approach)  
> **Tài liệu chi tiết:** Tham khảo toàn diện tại [docs/week02/08-week02-interview-cheatsheet.md](file:///Users/anhtus/Documents/Development/DE/GRT_DA-DE/docs/week02/08-week02-interview-cheatsheet.md)  

---

## 🎯 1. TỔNG QUAN PHẢN XẠ KỸ THUẬT

Tài liệu này tổng hợp 5 câu hỏi phỏng vấn trọng điểm nhất của Tuần 02, được cấu trúc theo tiêu chuẩn sư phạm của Anh Tú:
* **Definition Anatomy:** Định nghĩa chuẩn học thuật và giải phẫu từ khóa cấu thành.
* **Root Cause Analysis:** Lý giải nguyên nhân sâu xa từ kiến trúc hệ thống thực tế.
* **Trade-Off Analysis:** Phân tích sự đánh đổi được - mất của các giải pháp.
* **Quantified Evidence:** Dẫn chứng số liệu thực chứng đo lường từ 1.281.490 bản ghi của Olist.

---

## 💼 2. BỘ 5 CÂU HỎI & CÂU TRẢ LỜI MẪU CẤP ĐỘ SENIOR

---

### ❓ Q1. Grain của Fact Table là gì? Bạn chọn Grain nào cho Olist và vì sao?

* **Definition Anatomy:** **Fact Table Grain (Hạt nhân bảng Fact)** là mức độ chi tiết nguyên tử đại diện cho đúng một dòng dữ liệu duy nhất trong bảng Fact. Đây là quyết định kiến trúc quan trọng nhất trong mô hình hóa chiều Kimball, phải được chốt trước khi chọn bất kỳ cột chiều hay độ đo nào.
* **Lựa chọn cho Olist:** BẮT BUỘC chọn **Order Item Grain** (1 dòng = 1 sản phẩm trong 1 đơn hàng: `order_id + order_item_id`).
* **Root Cause & Trade-off:** 
  * Đây là mức độ chi tiết nguyên tử nhất có giá sản phẩm (`price`) và cước phí vận chuyển riêng lẻ (`freight_value`).
  * Trên sàn thương mại điện tử Olist, một đơn hàng có thể chứa nhiều sản phẩm đến từ nhiều nhà bán lẻ (Sellers) khác nhau và thuộc nhiều danh mục (Categories) khác nhau.
  * Nếu chọn Grain theo đơn hàng (`order_id`), ta sẽ mất hoàn toàn khả năng phân tích đa chiều theo Seller và Category. Từ Order Item Grain, ta luôn có thể cuộn lên (Roll-up) thành bất kỳ cấp độ tổng hợp nào mà không mất thông tin.

---

### ❓ Q2. Vì sao BẮT BUỘC phải làm DE EDA trước khi viết DQ Rules? Phân biệt DE EDA vs DS EDA?

* **Definition Anatomy:** **DE EDA (Kỹ nghệ khám nghiệm dữ liệu)** là quy trình phân tích định lượng về các thuộc tính vật lý, ranh giới miền giá trị (Domain Boundaries), tính nhất quán thời gian và tính toàn vẹn liên nguồn nhằm xác lập tham số thực chứng cho Data Contract, Schema Sizing, Ingestion Pattern và Data Quality Catalog.
* **Khác biệt với DS EDA:** DS EDA tập trung vào tìm mối tương quan (Correlation) và trích xuất đặc trưng cho Machine Learning. Ngược lại, DE EDA kiểm tra độ dài chuỗi (Memory Sizing) và các giá trị biên (0, âm, NULL) để bảo đảm pipeline vận hành kiên cường.
* **Thực chứng Olist:** 
  * EDA đo được 9 giao dịch có `payment_value = 0.0` do dùng voucher giảm giá 100%. Nếu đặt rule `payment_value > 0` mức Critical thì pipeline sẽ bị sập oan uổng.
  * EDA phát hiện 610 sản phẩm (~1.85%) khuyết `product_category_name`. Do đó, DE không đặt rule cấm null mức Critical mà map về `'Unknown'` và chỉ cảnh báo khi tỷ lệ null vượt quá 3.0%.

---

### ❓ Q3. Tài liệu Source-to-Target Mapping (STTM) dùng để làm gì trong dự án Data Lakehouse thực tế?

* **Definition Anatomy:** **STTM (Bản đồ ánh xạ dữ liệu)** là bản hợp đồng dữ liệu kỹ thuật ràng buộc giữa bộ phận nghiệp vụ (DA/BI/Product) và đội ngũ kỹ thuật (DE), quy định chi tiết mối quan hệ chuyển dịch từ trường dữ liệu nguồn sang trường dữ liệu đích ở các tầng Bronze, Silver, Gold.
* **Nội dung bắt buộc:** Bảng đích, Cột đích, Kiểu dữ liệu, Bảng nguồn, Cột nguồn, Quy tắc chuyển đổi (Transformation Rules), Bộ lọc chất lượng (DQ Filters) và Phân loại an ninh bảo mật (Sensitivity).
* **Giá trị thực tế:** STTM là kim chỉ nam duy nhất để DE viết code pipeline, QA viết test tự động và phục vụ kiểm toán truy vết nguồn gốc dữ liệu (Data Lineage & Governance).

---

### ❓ Q4. Phân biệt `customer_id` và `customer_unique_id` trong kiến trúc Olist? Ảnh hưởng gì đến thiết kế?

* **Bản chất hệ thống nguồn:**
  * `customer_id` là mã phiên giao dịch sinh mới cho mỗi đơn hàng (1 người mua 5 lần sẽ có 5 `customer_id` khác nhau).
  * `customer_unique_id` mới là định danh cố định của một cá nhân ngoài đời thực (gắn với mã số thuế cá nhân CPF).
* **Thực chứng từ EDA:** Có **99.441** `customer_id` nhưng chỉ có **96.096** `customer_unique_id`. Số khách hàng mua từ lần 2 trở lên là **3.345 người (~3.36% độ lệch)**.
* **Tác động sống còn:**
  * Khóa tự nhiên của bảng chiều khách hàng `dim_customer` bắt buộc phải là `customer_unique_id`.
  * Khi tính Customer Lifetime Value (LTV) và phân khúc RFM, bắt buộc phải gom nhóm theo `customer_unique_id`. Nếu dùng nhầm `customer_id`, hệ thống sẽ hiểu sai rằng mọi khách hàng chỉ mua 1 lần, làm tê liệt hoàn toàn các mô hình phân tích giữ chân khách hàng.

---

### ❓ Q5. Vì sao bảo vệ dữ liệu PII phải dùng HMAC-SHA256 có Secret Key thay vì SHA-256 trần?

* **Lỗ hổng của SHA-256 trần:** Không gian giá trị của email và số điện thoại là rất nhỏ (Entropy thấp). Hacker chỉ cần vài giờ dùng GPU để tạo bảng từ điển băm sẵn (**Rainbow Table**) của hàng trăm triệu email và số điện thoại phổ biến, sau đó tra cứu ngược để giải mã danh tính khách hàng trong nháy mắt.
* **Sức mạnh phòng vệ của HMAC-SHA256:** Bổ sung Khóa bí mật (Secret Key) được lưu trữ an toàn trong KMS hoặc biến môi trường. Vì kẻ tấn công không biết Secret Key, mọi bảng từ điển băm sẵn đều trở nên vô giá trị 100%.
* **Tính tất định (Deterministic):** Cùng một email + cùng secret key luôn sinh ra cùng 1 token duy nhất. Điều này cho phép đội ngũ phân tích vẫn JOIN bảng, tính toán RFM và LTV chính xác mà không bao giờ để lộ thông tin định danh cá nhân thật của khách hàng, tuân thủ tuyệt đối quy định pháp lý LGPD/GDPR.

---
*Made by Anh Tu - Share to be share*
