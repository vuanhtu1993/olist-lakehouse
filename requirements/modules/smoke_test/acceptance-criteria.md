# Module smoke_test — Acceptance Criteria & Verification

> **Parent Document:** [Module Spec](spec.md) | [Pipeline Contract](api-contract.md)  
> **Module ID:** `SMOKE` | **Standard:** Gherkin Behavioral Specification

---

## AC-SMOKE-01: Spark trích xuất dữ liệu Postgres thành công
- **Mapped FR:** [`FR-SMOKE-01`](spec.md#2-functional-requirements)

```gherkin
Scenario: Spark đọc bảng orders từ Postgres qua JDBC
  Given Postgres container đang chạy và bảng "orders" có dữ liệu
  When Spark thực thi lệnh đọc DataFrame qua JDBC
  Then DataFrame được tạo thành công
  And phương thức "df_orders.count()" trả về số nguyên > 0
  And schema của DataFrame phản ánh chính xác các cột của bảng Postgres
```

---

## AC-SMOKE-02: Spark tạo namespace và ghi Iceberg table lên MinIO
- **Mapped FR:** [`FR-SMOKE-02`](spec.md#2-functional-requirements)
- **Governing Rule:** [`BR-M0-05`](../../global/business-rules.md#br-m0-05)

```gherkin
Scenario: Spark ghi bảng Iceberg thông qua Polaris REST catalog
  Given Spark đã kết nối thành công tới Polaris catalog
  When Spark thực thi câu lệnh tạo namespace "lakehouse.bronze" và ghi dữ liệu "writeTo('lakehouse.bronze.oms_orders_smoke').append()"
  Then quá trình ghi hoàn thành không có ngoại lệ (No Exception)
  And Polaris REST catalog ghi nhận snapshot mới cho bảng "bronze.oms_orders_smoke"
```

---

## AC-SMOKE-03: Trino truy vấn bảng Iceberg vừa tạo
- **Mapped FR:** [`FR-SMOKE-03`](spec.md#2-functional-requirements)

```gherkin
Scenario: Trino đọc dữ liệu Iceberg thông qua catalog
  Given bảng "lakehouse.bronze.oms_orders_smoke" đã được Spark commit
  When tôi kết nối Trino và chạy "SELECT * FROM lakehouse.bronze.oms_orders_smoke LIMIT 5;"
  Then Trino trả về chính xác 5 dòng dữ liệu với đầy đủ các cột nghiệp vụ và cột "_ingest_timestamp"
```

---

## AC-SMOKE-04: Tự động đối soát số lượng bản ghi (Tie-out 100%)
- **Mapped FR:** [`FR-SMOKE-04`](spec.md#2-functional-requirements)

```gherkin
Scenario: Số lượng bản ghi tại đích (Trino/Iceberg) khớp 100% với nguồn (Postgres)
  Given số lượng bản ghi trong Postgres được đo bằng "SELECT count(*) FROM orders;" là N
  When tôi thực hiện truy vấn trên Trino "SELECT count(*) FROM lakehouse.bronze.oms_orders_smoke;"
  Then kết quả trả về từ Trino chính xác bằng N (Chênh lệch = 0)
```

---

## AC-SMOKE-05: Kiểm tra tính toàn vẹn của tệp tin trên MinIO Object Storage
- **Mapped FR:** [`FR-SMOKE-05`](spec.md#2-functional-requirements)

```gherkin
Scenario: Tệp tin metadata và parquet xuất hiện đúng cấu trúc trên MinIO
  Given job Spark đã commit thành công
  When tôi kiểm tra bucket "bronze" trên MinIO qua Web Console hoặc S3 API
  Then prefix "bronze/oms_orders_smoke/metadata/" chứa ít nhất một tệp tin ".metadata.json" và các tệp tin manifest ".avro"
  And prefix "bronze/oms_orders_smoke/data/" chứa các tệp tin định dạng ".parquet"
```

---
*Made by Anh Tu - Share to be share*
