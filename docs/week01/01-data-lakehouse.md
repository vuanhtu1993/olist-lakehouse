# Module 01: Foundations of Modern Data Lakehouse

> **Target Audience:** IT Students & Software Engineers transitioning to Data Engineering  
> **Topic Scope:** Data Lakehouse Architecture, Storage-Compute Decoupling, Medallion Architecture, ETL vs ELT  
> **Estimated Reading Time:** 15 minutes  

---

## 1. The Big Picture: Why Do We Need a Lakehouse?

In software development, you are familiar with relational databases like PostgreSQL or MySQL. They handle transactions (OLTP) exceptionally well. However, when an enterprise scales to petabytes of historical data, analytics queries (OLAP) will slow down or crash your production databases.

To solve this, data architecture has evolved through three major generations:

```mermaid
graph TD
    subgraph Gen1 ["1. Traditional Data Warehouse (1990s - 2010s)"]
        DW1["Coupled Storage + Compute (e.g., Oracle, Teradata)"]
        DW2["Rigid Schema-on-Write, Strong ACID"]
        DW3["Limitation: Prohibitive cost, vendor lock-in, structured data only"]
    end

    subgraph Gen2 ["2. Data Lake Generation (2010s - 2020)"]
        DL1["Cheap Cloud Storage: HDFS, AWS S3, MinIO"]
        DL2["Store anything: Raw CSV, JSON, Parquet"]
        DL3["Limitation: No ACID transactions, dirty data, 'Data Swamp'"]
    end

    subgraph Gen3 ["3. Modern Data Lakehouse (2020 - Present)"]
        LH1["Storage: Cheap Object Storage (S3 / MinIO)"]
        LH2["Table Format: Apache Iceberg (ACID, Schema Evolution, Time Travel)"]
        LH3["Compute: Decoupled Engines (Spark for ETL, Trino for SQL)"]
        LH4["Result: Low cost + High scalability + Enterprise ACID reliability"]
    end

    Gen1 --> Gen3
    Gen2 --> Gen3

    classDef legacy fill:#ffebee,stroke:#e53935,stroke-width:2px,color:#b71c1c;
    classDef intermediate fill:#fff8e1,stroke:#fbc02d,stroke-width:2px,color:#f57f17;
    classDef modern fill:#e8f5e9,stroke:#43a047,stroke-width:2px,color:#1b5e20;

    class DW1,DW2,DW3 legacy;
    class DL1,DL2,DL3 intermediate;
    class LH1,LH2,LH3,LH4 modern;
```

---

## 2. Concept Anatomy: Data Lakehouse

### Academic Definition
> A **Data Lakehouse** is an *open data management architecture* that combines the flexibility, cost-efficiency, and scalability of a **Data Lake** with the data structure, transaction reliability (ACID), and query performance of a **Data Warehouse**.

### Keyword Dissection (Anatomy)

```mermaid
flowchart TD
    Root["Data Lakehouse Core Anatomy"]

    subgraph Formats ["Open Storage & Table Formats"]
        F1["Apache Parquet<br/>Columnar File Format, Snappy/ZSTD compression"]
        F2["Apache Iceberg<br/>Open Table Format: Snapshots, Schema Evolution, ACID"]
    end

    subgraph Architecture ["Decoupled Infrastructure"]
        A1["Storage Plane<br/>Object Storage: MinIO / AWS S3"]
        A2["Compute Plane<br/>Stateless Engines: Apache Spark, Trino"]
    end

    subgraph Control ["Centralized Catalog Layer"]
        C1["Apache Polaris<br/>REST Catalog Protocol & Security Access"]
        C2["Atomic Metadata Pointer<br/>Optimistic Concurrency Control (OCC)"]
    end

    Root --> Formats
    Root --> Architecture
    Root --> Control

    classDef rootNode fill:#1e293b,stroke:#0f172a,stroke-width:2px,color:#ffffff;
    classDef formatGroup fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef archGroup fill:#f0fdf4,stroke:#16a34a,stroke-width:2px,color:#15803d;
    classDef controlGroup fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#b45309;

    class Root rootNode;
    class F1,F2 formatGroup;
    class A1,A2 archGroup;
    class C1,C2 controlGroup;
```

1. **Open Storage & Table Format:**  
   Instead of storing data in proprietary binary databases, files are stored as standard **Apache Parquet** files and managed by **Apache Iceberg**. Any modern engine (Python, Spark, Trino, DuckDB) can read the exact same data files.
2. **ACID Transactions:**  
   Guarantees *Atomicity, Consistency, Isolation, and Durability*. If a Spark job fails halfway through writing 1,000,000 rows, partial writes are discarded cleanly. Readers never see corrupted or partial data.
3. **Storage-Compute Decoupling:**  
   Storage and compute scale independently. You pay pennies per gigabyte for storage on S3/MinIO, and compute nodes (Spark/Trino) are only spun up when needed.

---

## 3. Architectural Principle: Storage-Compute Decoupling

In traditional RDBMS (like local PostgreSQL), compute (CPU/RAM) and storage (SSD) live on the same machine. If disk space runs out, you must upgrade the entire instance—even if your CPU usage is only at 5%.

### How Decoupling Works in Lakehouse:

```mermaid
flowchart TD
    subgraph ComputeLayer ["Compute Layer (Stateless Engines)"]
        Spark["Apache Spark<br/>(Heavy Batch Processing / ETL)"]
        Trino["Trino SQL<br/>(Interactive BI & Ad-hoc Queries)"]
    end

    subgraph CatalogLayer ["Control Plane (Metadata Management)"]
        Polaris["Apache Polaris (REST Catalog)<br/>Maintains atomic pointers to latest table snapshots"]
    end

    subgraph StorageLayer ["Data Plane (Stateful Object Storage)"]
        MinIO["MinIO S3 Object Storage<br/>Raw Data & Metadata Files (.parquet, .avro, .json)"]
    end

    Spark -->|"1. Request active snapshot"| Polaris
    Trino -->|"1. Request active snapshot"| Polaris
    Spark -->|"2. Read & Write Parquet/Avro directly"| MinIO
    Trino -->|"2. Read Parquet data directly"| MinIO

    classDef computeStyle fill:#e0e7ff,stroke:#4338ca,stroke-width:2px,color:#312e81;
    classDef catalogStyle fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e;
    classDef storageStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;

    class Spark,Trino computeStyle;
    class Polaris catalogStyle;
    class MinIO storageStyle;
```

### Dev-to-DE Analogy:
* Think of **Storage-Compute Decoupling** like **Stateless Web Services vs. Database Storage** in backend engineering:
  * Your web API containers (Compute) can scale up to 50 pods or scale down to 0 depending on traffic.
  * Your persistent data (Storage) remains safe in object storage without paying for idle CPUs.

---

## 4. Data Quality Framework: Medallion Architecture

In software engineering, you never display raw API inputs directly onto a user's dashboard without validation and sanitization. In Data Engineering, the **Medallion Architecture** (Multi-Hop Architecture) enforces this exact data hygiene practice across three progressive tiers:

```mermaid
flowchart LR
    Source[("Source Systems<br/>(OLTP Postgres / Files)")] -->|"Raw Ingestion"| Bronze["Bronze Layer<br/>Raw / Immutable"]
    Bronze -->|"Clean & Standardize"| Silver["Silver Layer<br/>Enriched / Validated"]
    Silver -->|"Aggregate & Model"| Gold["Gold Layer<br/>Curated / Star Schema"]

    Gold --> BI["BI Dashboards<br/>& Data Analysts"]
    Gold --> ML["Machine Learning<br/>& Data Science"]

    classDef srcStyle fill:#f1f5f9,stroke:#64748b,stroke-width:2px,color:#334155;
    classDef bronzeStyle fill:#efebe9,stroke:#8d6e63,stroke-width:2px,color:#3e2723;
    classDef silverStyle fill:#eceff1,stroke:#78909c,stroke-width:2px,color:#263238;
    classDef goldStyle fill:#fff8e1,stroke:#ffb300,stroke-width:2px,color:#e65100;
    classDef consumerStyle fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;

    class Source srcStyle;
    class Bronze bronzeStyle;
    class Silver silverStyle;
    class Gold goldStyle;
    class BI,ML consumerStyle;
```

### Layer Breakdown & Responsibilities

| Dimension | Bronze Layer | Silver Layer | Gold Layer |
| :--- | :--- | :--- | :--- |
| **Data Nature** | Raw, immutable append-only copy of source | Cleansed, validated, normalized, deduplicated | Aggregated, denormalized, Star Schema |
| **Transformation Level** | None (1:1 with source) + ingestion timestamp | Type casting, deduplication, handling nulls, SCD Type 2 | Dimensional modeling (Facts & Dimensions), KPI metrics |
| **PII Handling** | Raw PII masked or tokenized | PII hashed or isolated | Strictly no raw PII |
| **Primary Audience** | Data Engineers (auditing & backfilling) | Data Engineers, Data Scientists | Data Analysts, BI Dashboards, Business Executives |
| **Olist Project Example** | `lakehouse.bronze.orders` (Raw copy from Postgres) | `lakehouse.silver.orders` (Clean dates, valid status) | `lakehouse.gold.kpi_monthly_revenue` |

---

## 5. Paradigm Shift: ETL vs ELT

When designing data pipelines, you will encounter two fundamental execution models: **ETL** and **ELT**.

```mermaid
graph TD
    subgraph ETL ["Traditional ETL (Extract -> Transform -> Load)"]
        E1["Extract from Source"] --> T1["Transform in Separate Compute Server<br/>(Heavy memory bottleneck)"]
        T1 --> L1["Load into Data Warehouse"]
    end

    subgraph ELT ["Modern ELT (Extract -> Load -> Transform)"]
        E2["Extract from Source"] --> L2["Load directly into Lakehouse Bronze<br/>(Raw preservation)"]
        L2 --> T2["Transform inside Lakehouse via Spark/SQL<br/>(Scalable distributed compute)"]
    end

    classDef etlWarn fill:#ffebee,stroke:#ef5350,stroke-width:2px,color:#c62828;
    classDef eltGood fill:#e8f5e9,stroke:#66bb6a,stroke-width:2px,color:#2e7d32;
    classDef neutral fill:#f8fafc,stroke:#94a3b8,stroke-width:1px,color:#1e293b;

    class E1,L1,E2 neutral;
    class T1 etlWarn;
    class L2,T2 eltGood;
```

### Why Did the Industry Shift from ETL to ELT?

* **Root Cause (Historical Context):**  
  In the 1990s, storage was extremely expensive ($10,000+ per gigabyte). Enterprises could not afford to store raw, unformatted data. They had to discard raw records and only store pre-aggregated summary tables.
* **Modern Reality:**  
  Cloud storage is virtually free (~$0.02 per GB per month on S3). Distributed compute (Spark) can process terabytes in parallel.
* **The Key Advantage of ELT:**  
  **Auditing and Reprocessing (Zero Data Loss).** If business logic changes or a bug is discovered in month 6, in ELT you simply re-run your transformation job from the raw Bronze layer. In ETL, because raw data was discarded during extraction, re-running is impossible without re-extracting from the production database.

---

## 6. Trade-Off Analysis & Critical Thinking

Every engineering decision involves trade-offs. The 2x2 matrix below maps architectural options by **Scalability** versus **Operational Simplicity**:

```mermaid
flowchart TB
    subgraph HighScale ["High Scalability"]
        direction LR
        subgraph Quadrant_Lakehouse ["Modern Data Lakehouse (Iceberg + Spark)"]
            direction TB
            LH_T["High Scalability + High Architecture Rigor"]
            LH_D["• Low storage cost on S3/MinIO<br/>• Full ACID, schema evolution & time travel<br/>• Multi-engine support: Spark for ETL, Trino for BI<br/>• <b>Trade-off:</b> Requires metadata catalog orchestration"]
        end

        subgraph Quadrant_RawLake ["Raw Data Lake (S3 + Parquet / CSV)"]
            direction TB
            RL_T["High Scalability + High Storage Simplicity"]
            RL_D["• Extremely cheap storage<br/>• Easy to write raw files<br/>• <b>Trade-off:</b> No ACID, dirty files, high risk of Data Swamp"]
        end
    end

    subgraph LowScale ["Low Scalability"]
        direction LR
        subgraph Quadrant_Warehouse ["Traditional Data Warehouse"]
            direction TB
            DW_T["Coupled Hardware + High Maintenance"]
            DW_D["• Powerful SQL optimizer<br/>• Strong ACID guarantees<br/>• <b>Trade-off:</b> High license cost, storage coupled to compute"]
        end

        subgraph Quadrant_RDBMS ["Single-Node RDBMS (PostgreSQL)"]
            direction TB
            PG_T["Low Scalability + Maximum Simplicity"]
            PG_D["• Ideal for OLTP (< 100GB)<br/>• Standard SQL, zero setup overhead<br/>• <b>Trade-off:</b> Fails on analytical aggregations at scale"]
        end
    end

    classDef lakehouseStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;
    classDef rawLakeStyle fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e;
    classDef warehouseStyle fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#991b1b;
    classDef rdmsStyle fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;

    class LH_T,LH_D lakehouseStyle;
    class RL_T,RL_D rawLakeStyle;
    class DW_T,DW_D warehouseStyle;
    class PG_T,PG_D rdmsStyle;
```

### Architectural Decision Guide

| Strategy | When to Choose | When to Avoid (Trade-Off) |
| :--- | :--- | :--- |
| **Pure PostgreSQL (OLTP only)** | Small datasets (< 100GB), simple CRUD applications. | Large-scale aggregations, historical audit logs, petabyte analytics. |
| **Data Lake without Iceberg (Raw S3 + Parquet)** | Ad-hoc machine learning training, simple file archiving. | Concurrent writes, row-level updates/deletes, ACID guarantees needed. |
| **Data Lakehouse (Iceberg + MinIO + Spark)** | Enterprise multi-tier analytics, multiple concurrent compute engines, auditable data. | Small teams needing a simple dashboard within 1 hour; requires orchestration overhead. |

---

## 7. Practical Connection: Our Olist Project Mapping

Here is how each theory concept maps to the running code in our repository:

| Theory Concept | Implementation in `olist-lakehouse` |
| :--- | :--- |
| **Storage Engine (Data Plane)** | **MinIO** running at `http://localhost:9000` (Buckets: `landing`, `bronze`, `silver`, `gold`) |
| **Table Format** | **Apache Iceberg 1.6** storing `.parquet` data and `.avro` snapshots |
| **Metadata Catalog (Control Plane)** | **Apache Polaris REST** running at `http://localhost:8181` |
| **Compute - Ingestion & ETL** | **PySpark 3.5** executing jobs from [spark_jobs/](file:///Users/anhtus/Documents/Development/DE/GRT_DA-DE/spark_jobs) |
| **Compute - Interactive Analytics** | **Trino 442** running at `http://localhost:8080` querying catalog `lakehouse` |

---

## 8. Knowledge Check (Self-Assessment)

1. **Question:** *If MinIO stores both `.parquet` data files and `.json` metadata files, why can't MinIO itself guarantee ACID transactions?*  
   * **Answer:** MinIO is a simple Key-Value store; it does not understand table states or atomic commits. The ACID guarantee is managed by **Apache Iceberg** writing atomic snapshot files, with **Polaris Catalog** atomically swapping the pointer to the latest valid metadata file.

2. **Question:** *Why don't we clean, remove duplicates, and cast datatypes directly when writing into the Bronze layer?*  
   * **Answer:** The Bronze layer must remain an exact 1:1 raw copy of the source system. Preserving raw data guarantees an immutable audit trail and enables data engineers to re-play and re-process historical data if business logic changes.

3. **Question:** *Can Trino and Apache Spark read from the same Iceberg table at the same time?*  
   * **Answer:** Yes! Because metadata is centrally managed by Polaris REST Catalog, Spark can write a new snapshot while Trino safely reads from the current committed snapshot using Optimistic Concurrency Control (OCC).

---
*Made by Anh Tu - Share to be share*
