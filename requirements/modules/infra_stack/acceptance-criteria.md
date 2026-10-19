# Module infra_stack — Acceptance Criteria & Verification

> **Parent Document:** [Module Spec](spec.md) | [Service Contract](api-contract.md)  
> **Module ID:** `INFRA` | **Standard:** Gherkin Behavioral Specification

---

## AC-INFRA-01: Khởi chạy cụm Core container và Giới hạn bộ nhớ RAM
- **Mapped FR:** [`FR-INFRA-01`](spec.md#2-functional-requirements)
- **Mapped Story:** [`US-INFRA-01`](spec.md#us-infra-01)
- **Governing Rule:** [`BR-M0-01`](../../global/business-rules.md#br-m0-01)

```gherkin
Scenario: Khởi động hệ thống với Docker Profile core thành công
  Given máy tính đã cài đặt Docker và Docker Compose v2
  And không có tiến trình nào đang chiếm dụng các cổng 5432, 9000, 9001, 8181, 8080, 8888
  When tôi thực hiện lệnh "docker compose --profile core up -d"
  Then 5 container ("olist-postgres", "olist-minio", "olist-polaris", "olist-spark", "olist-trino") đều ở trạng thái "Up" hoặc "healthy"
  And lệnh "docker stats --no-stream --format '{{.MemUsage}}'" cho thấy tổng dung lượng RAM tiêu thụ không vượt quá 6.0 GiB
  And container "airflow" và "superset" không được khởi chạy
```

---

## AC-INFRA-02: Kiểm chứng tính bền vững dữ liệu qua chu kỳ Restart (Persistence)
- **Mapped FR:** [`FR-INFRA-02`](spec.md#2-functional-requirements)
- **Governing Rule:** [`BR-M0-02`](../../global/business-rules.md#br-m0-02)

```gherkin
Scenario: Dữ liệu Postgres và MinIO không bị mất khi container down
  Given container "olist-postgres" đã có database "oltp_olist" chứa bảng dữ liệu
  And container "olist-minio" đã có các bucket dữ liệu
  When tôi thực hiện lệnh "docker compose down"
  And sau đó tôi thực hiện lệnh "docker compose --profile core up -d"
  Then dữ liệu trong bảng của Postgres vẫn truy vấn được mà không cần seed lại
  And các bucket trong MinIO vẫn tồn tại nguyên vẹn
```

---

## AC-INFRA-03: Kiểm tra khởi tạo 4 Buckets trên MinIO
- **Mapped FR:** [`FR-INFRA-03`](spec.md#2-functional-requirements)

```gherkin
Scenario: MinIO có đủ 4 bucket phục vụ kiến trúc Medallion
  Given MinIO container đang hoạt động
  When tôi kiểm tra danh sách buckets qua AWS CLI / MinIO Client / MinIO Console
  Then tôi quan sát thấy chính xác 4 bucket: "landing", "bronze", "silver", "gold"
```

---

## AC-INFRA-04: Trino nhận diện Iceberg REST Catalog
- **Mapped FR:** [`FR-INFRA-04`](spec.md#2-functional-requirements)

```gherkin
Scenario: Trino kết nối thành công tới Polaris REST Catalog
  Given container "olist-trino" và "olist-polaris" đều đang chạy
  When tôi kết nối vào Trino CLI và thực thi câu lệnh "SHOW CATALOGS;"
  Then kết quả trả về trong danh sách catalog có chứa giá trị "lakehouse"
```

---

## AC-INFRA-05: Spark Session kết nối Iceberg Catalog thành công
- **Mapped FR:** [`FR-INFRA-05`](spec.md#2-functional-requirements)

```gherkin
Scenario: PySpark khởi tạo SparkCatalog thành công
  Given Spark container đang chạy với cấu hình Iceberg REST catalog
  When tôi chạy lệnh Python "spark.sql('SHOW NAMESPACES IN lakehouse').show()"
  Then lệnh thực thi thành công không phát sinh Exception
```

---
*Made by Anh Tu - Share to be share*
