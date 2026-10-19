# Governance 02 — Requirements Traceability Matrix (RTM): Olist-Lakehouse-M0

> **Parent Document:** [Requirement Analysis Package](../README.md)  
> **Version:** 1.0 (Baseline)  
> **Architecture Layer:** `governance/` (Quality Control & Architecture)

---

## 1. Bi-directional Traceability Matrix (RTM)

Bảng ma trận truy vết hai chiều liên kết từ Yêu cầu chức năng (FR) đến User Story, Tiêu chí nghiệm thu (AC), Contract kỹ thuật và Kịch bản kiểm thử:

| FR ID | Module | User Story | Acceptance Criteria (AC) | Mapped Interface / Contract | Automated Test / Verification Script |
|---|---|---|---|---|---|
| **FR-INFRA-01** | `infra_stack` | `US-INFRA-01` | [`AC-INFRA-01`](../modules/infra_stack/acceptance-criteria.md#ac-infra-01) | [`Core Services`](../modules/infra_stack/api-contract.md#1-core-services-endpoint--port-contract) | `docker compose --profile core up -d && docker stats` |
| **FR-INFRA-02** | `infra_stack` | `US-INFRA-01` | [`AC-INFRA-02`](../modules/infra_stack/acceptance-criteria.md#ac-infra-02) | [`Volumes`](../modules/infra_stack/api-contract.md#2-docker-volumes-and-mount-points) | `docker compose down && docker compose up -d` |
| **FR-INFRA-03** | `infra_stack` | `US-INFRA-01` | [`AC-INFRA-03`](../modules/infra_stack/acceptance-criteria.md#ac-infra-03) | [`MinIO S3`](../modules/infra_stack/api-contract.md#1-core-services-endpoint--port-contract) | `python tests/verify_buckets.py` |
| **FR-INFRA-04** | `infra_stack` | `US-INFRA-01` | [`AC-INFRA-04`](../modules/infra_stack/acceptance-criteria.md#ac-infra-04) | [`Trino Catalog`](../modules/infra_stack/api-contract.md#3-trino-iceberg-catalog-contract-lakehouseproperties) | Trino CLI: `SHOW CATALOGS;` |
| **FR-INFRA-05** | `infra_stack` | `US-INFRA-01` | [`AC-INFRA-05`](../modules/infra_stack/acceptance-criteria.md#ac-infra-05) | [`Spark Conf`](../modules/infra_stack/api-contract.md#4-spark-iceberg-session-configuration-contract) | PySpark: `spark.sql('SHOW NAMESPACES IN lakehouse')` |
| **FR-SEED-01** | `data_seeding` | `US-SEED-01` | [`AC-SEED-01`](../modules/data_seeding/acceptance-criteria.md#ac-seed-01) | [`CLI Commands`](../modules/data_seeding/api-contract.md#1-cli-command-contracts) | `ls -la data_prep/raw/*.csv \| wc -l` (Trả về 9) |
| **FR-SEED-02** | `data_seeding` | `US-SEED-01` | [`AC-SEED-02`](../modules/data_seeding/acceptance-criteria.md#ac-seed-02) | [`PII Contract`](../modules/data_seeding/api-contract.md#2-customer-pii-data-contract-customer_piicsv) | `python data_prep/gen_pii.py && pytest tests/test_pii.py` |
| **FR-SEED-03** | `data_seeding` | `US-SEED-01` | [`AC-SEED-03`](../modules/data_seeding/acceptance-criteria.md#ac-seed-03) | [`Batch Slicing`](../modules/data_seeding/api-contract.md#3-batch-slicing-contract-m%E1%BB%91c-th%E1%BB%9Di-gian-b0b7) | `python data_prep/split_batches.py` |
| **FR-SEED-04** | `data_seeding` | `US-SEED-01` | [`AC-SEED-04`](../modules/data_seeding/acceptance-criteria.md#ac-seed-04) | [`Postgres DDL`](../modules/data_seeding/api-contract.md#4-postgresql-table-schemas-contract-oltp_olist) | `python data_prep/seed_postgres.py` |
| **FR-SEED-05** | `data_seeding` | `US-SEED-01` | [`AC-SEED-05`](../modules/data_seeding/acceptance-criteria.md#ac-seed-05) | [`MinIO Landing`](../modules/data_seeding/api-contract.md) | MinIO Client check bucket `landing/` |
| **FR-SMOKE-01** | `smoke_test` | `US-SMOKE-01` | [`AC-SMOKE-01`](../modules/smoke_test/acceptance-criteria.md#ac-smoke-01) | [`JDBC Contract`](../modules/smoke_test/api-contract.md#1-jdbc-source-connection-contract) | PySpark JDBC Read check `df.count() > 0` |
| **FR-SMOKE-02** | `smoke_test` | `US-SMOKE-01` | [`AC-SMOKE-02`](../modules/smoke_test/acceptance-criteria.md#ac-smoke-02) | [`Iceberg Write`](../modules/smoke_test/api-contract.md#2-iceberg-table-write-contract) | PySpark writeTo `.append()` |
| **FR-SMOKE-03** | `smoke_test` | `US-SMOKE-01` | [`AC-SMOKE-03`](../modules/smoke_test/acceptance-criteria.md#ac-smoke-03) | [`Trino Query`](../modules/smoke_test/api-contract.md#3-trino-query-and-tie-out-contract) | Trino CLI: `SELECT count(*) FROM ...` |
| **FR-SMOKE-04** | `smoke_test` | `US-SMOKE-01` | [`AC-SMOKE-04`](../modules/smoke_test/acceptance-criteria.md#ac-smoke-04) | [`Tie-out Contract`](../modules/smoke_test/api-contract.md#3-trino-query-and-tie-out-contract) | `notebooks/00_smoke_test.ipynb` (Postgres == Trino) |
| **FR-SMOKE-05** | `smoke_test` | `US-SMOKE-01` | [`AC-SMOKE-05`](../modules/smoke_test/acceptance-criteria.md#ac-smoke-05) | [`Storage Audit`](../modules/smoke_test/api-contract.md#4-minio-storage-audit-contract) | S3 CLI verify metadata and parquet files |

---
*Made by Anh Tu - Share to be share*
