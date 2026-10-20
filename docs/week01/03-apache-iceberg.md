# Module 03: Apache Iceberg — Open Table Format Deep Dive

> **Target Audience:** IT Students & Software Engineers transitioning to Data Engineering  
> **Topic Scope:** Table Format vs. File Format, Iceberg Metadata Tree, ACID Transactions, Hidden Partitioning, Schema Evolution, Time Travel  
> **Estimated Reading Time:** 18 minutes  

---

## 1. The Big Picture: File Format vs. Table Format

In [Module 02](file:///Users/anhtus/Documents/Development/DE/GRT_DA-DE/docs/week01/02-storage-formats.md), you learned that **Apache Parquet** is an outstanding columnar *file format*. It makes individual files fast and compact.

However, a production Data Lakehouse does not consist of just one file. A single production table contains **millions of Parquet files** spread across thousands of cloud storage directories.

This raises critical questions:
* *Which specific files belong to the table right now?*
* *What happens if a writer crashes while writing 50 new files?*
* *How do multiple compute engines (Spark and Trino) read and write concurrently without corrupting data?*

In the early days of Hadoop, the industry used directory-based tracking (Hive Metastore). If a directory contained a file, that file was assumed to be part of the table. This caused three fatal problems:
1. **No Atomic Commits:** Adding a partition required renaming files or directories on cloud storage, which is not atomic.
2. **Eventual Consistency Bugs:** Amazon S3 or MinIO directory listings could omit newly created files, resulting in missing query data.
3. **Slow Table Scans:** Finding which files to read required recursively listing thousands of directories (`O(N)` network roundtrips).

To fix this, the industry created **Open Table Formats**, led by **Apache Iceberg**.

```mermaid
flowchart TD
    subgraph FileLevel ["File Format Layer (Parquet / ORC)"]
        F1["data_01.parquet"]
        F2["data_02.parquet"]
        F3["data_03.parquet"]
        F_Note["Understands: Column layout, Row Groups, compression, chunk min/max stats.<br/>Does NOT understand: Table state, multi-file transactions, schema history."]
    end

    subgraph TableLevel ["Table Format Layer (Apache Iceberg)"]
        T_Meta["Table Metadata & Snapshot Tree"]
        T_Note["Understands: Which exact files compose the table, ACID transactions,<br/>schema evolution, partition evolution, time travel."]
    end

    TableLevel --> FileLevel

    classDef fileStyle fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef tableStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;
    classDef noteStyle fill:#f8fafc,stroke:#94a3b8,stroke-width:1px,color:#334155;

    class F1,F2,F3 fileStyle;
    class T_Meta tableStyle;
    class F_Note,T_Note noteStyle;
```

---

## 2. Concept Anatomy: Apache Iceberg

### Academic Definition
> **Apache Iceberg** is an *open-source, high-performance table format for huge analytic tables* that provides SQL table abstractions on top of object storage, enabling full ACID transactions, snapshot isolation, schema and partition evolution, and fast metadata-driven query pruning.

### Keyword Dissection (Anatomy)

```mermaid
flowchart TD
    Root["Apache Iceberg Core Anatomy"]

    subgraph TreeArchitecture ["1. Hierarchical Metadata Tree"]
        M1["Table Metadata (.json): Table schema, partition specs, snapshot log"]
        M2["Manifest List (.avro): List of manifests for a specific snapshot"]
        M3["Manifest File (.avro): List of data files with partition & column stats"]
    end

    subgraph TransactionEngine ["2. Transaction & Isolation"]
        T1["Snapshot Isolation: Readers always read a consistent committed snapshot"]
        T2["Optimistic Concurrency Control (OCC): Atomic pointer swap at commit"]
        T3["Zero-Copy Clones & Time Travel: Query historical snapshots via timestamp"]
    end

    subgraph EvolutionCapabilities ["3. In-Place Evolution"]
        E1["Schema Evolution: Add, rename, drop columns without rewriting files"]
        E2["Hidden Partitioning: Query engines prune partitions without explicit user filters"]
    end

    Root --> TreeArchitecture
    Root --> TransactionEngine
    Root --> EvolutionCapabilities

    classDef rootNode fill:#1e293b,stroke:#0f172a,stroke-width:2px,color:#ffffff;
    classDef treeGroup fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef transGroup fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;
    classDef evolGroup fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#b45309;

    class Root rootNode;
    class M1,M2,M3 treeGroup;
    class T1,T2,T3 transGroup;
    class E1,E2,E3 evolGroup;
```

---

## 3. The Iceberg Metadata Architecture (Tree Structure)

Iceberg organizes metadata into a three-tiered tree structure. This tree guarantees that **every read and write is tracked by explicit file paths**, never by directory listings.

```mermaid
flowchart TB
    Catalog["Catalog (e.g., Apache Polaris)<br/>Points atomically to current metadata file"]

    subgraph MetadataLayer ["1. Table Metadata Layer"]
        TM["v2.metadata.json<br/>• Current schema<br/>• Current partition spec<br/>• Snapshot log: [Snap 1, Snap 2]"]
    end

    subgraph ManifestListLayer ["2. Manifest List Layer"]
        ML["snap-9872.avro<br/>Represents Snapshot 2<br/>Lists all active manifest files with partition summaries"]
    end

    subgraph ManifestFileLayer ["3. Manifest File Layer"]
        MF1["manifest-a.avro<br/>Partition: year=2018/month=01<br/>Data file stats (min/max)"]
        MF2["manifest-b.avro<br/>Partition: year=2018/month=02<br/>Data file stats (min/max)"]
    end

    subgraph DataFileLayer ["4. Data Layer (Physical Storage)"]
        DF1["file_01.parquet"]
        DF2["file_02.parquet"]
        DF3["file_03.parquet"]
    end

    Catalog -->|"Points to latest"| TM
    TM -->|"Points to current snapshot"| ML
    ML --> MF1
    ML --> MF2
    MF1 --> DF1
    MF1 --> DF2
    MF2 --> DF3

    classDef catStyle fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e;
    classDef metaStyle fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b;
    classDef mlStyle fill:#e0e7ff,stroke:#4338ca,stroke-width:2px,color:#312e81;
    classDef mfStyle fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef dfStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;

    class Catalog catStyle;
    class TM metaStyle;
    class ML mlStyle;
    class MF1,MF2 mfStyle;
    class DF1,DF2,DF3 dfStyle;
```

### What Each Layer Does:
1. **Catalog:** Holds the single source of truth: the atomic pointer to the current `v<N>.metadata.json` file.
2. **Table Metadata (`*.metadata.json`):** Contains the complete table definition, table properties, all historical schemas, and a history of all snapshots.
3. **Manifest List (`snap-*.avro`):** Generated for every commit. It lists all manifest files that make up that snapshot, along with summary ranges for each partition.
4. **Manifest File (`*.avro`):** Tracks individual data files (`.parquet`), recording their exact file paths, partition values, and column-level lower/upper bounds.
5. **Data Files (`*.parquet`):** The actual analytical data files stored on MinIO or S3.

---

## 4. How ACID Transactions Work in Iceberg

In traditional databases, transactions use Write-Ahead Logs (WAL) and row-level locks. On cloud object storage, there are no distributed locks. Iceberg achieves ACID transactions through **Snapshot Isolation** and **Optimistic Concurrency Control (OCC)**.

```mermaid
sequenceDiagram
    autonumber
    participant Spark as Spark (Writer)
    participant Storage as MinIO Storage
    participant Catalog as Polaris Catalog
    participant Trino as Trino (Reader)

    Note over Spark,Trino: Initial State: Table is at Snapshot 1
    Trino->>Catalog: What is the current snapshot?
    Catalog-->>Trino: Snapshot 1 (v1.metadata.json)
    Trino->>Storage: Reading Snapshot 1 data safely in parallel...

    Note over Spark,Storage: Step 1: Write data files and new metadata
    Spark->>Storage: Write new Parquet files (file_new.parquet)
    Spark->>Storage: Write new manifest and snap-2.avro
    Spark->>Storage: Write v2.metadata.json

    Note over Spark,Catalog: Step 2: Atomic Commit (OCC)
    Spark->>Catalog: Commit: Change pointer from v1.metadata.json -> v2.metadata.json
    Catalog-->>Spark: 200 OK (Commit Successful)

    Note over Trino,Catalog: Step 3: Subsequent queries see new state
    Trino->>Catalog: What is the current snapshot?
    Catalog-->>Trino: Snapshot 2 (v2.metadata.json)
    Trino->>Storage: Now reads Snapshot 2!
```

### Why Readers Never Block Writers:
* When Spark writes 1,000,000 new rows, it writes them as **new files**. It never modifies existing Parquet files.
* Trino queries continue reading Snapshot 1 without interruption or locking.
* The moment the Catalog updates its pointer to `v2.metadata.json`, new queries immediately see Snapshot 2.
* If Spark crashes mid-write, the catalog pointer is never updated. The uncommitted Parquet files are simply orphaned and cleaned up later. **Readers never see dirty or partial data.**

---

## 5. Schema Evolution Without Table Rewrites

In traditional databases or Hive tables, renaming or dropping a column can require rewriting gigabytes of data or breaking historical partitions.

Iceberg uses **Unique Column IDs** instead of column names:

```mermaid
flowchart LR
    subgraph V1 ["Schema Version 1"]
        C1["ID: 1 | Name: 'customer_name' | Type: string"]
        C2["ID: 2 | Name: 'phone'         | Type: string"]
    end

    subgraph Operation ["User Action: RENAME 'customer_name' TO 'full_name'"]
        direction TB
        Op["No Parquet files are rewritten! Only metadata updates."]
    end

    subgraph V2 ["Schema Version 2"]
        C1_V2["ID: 1 | Name: 'full_name' | Type: string"]
        C2_V2["ID: 2 | Name: 'phone'     | Type: string"]
    end

    V1 --> Operation --> V2

    classDef v1Style fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef opStyle fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#b45309;
    classDef v2Style fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;

    class C1,C2 v1Style;
    class Op opStyle;
    class C1_V2,C2_V2 v2Style;
```

* When reading historical Parquet files created under Version 1, Iceberg matches data by **Column ID (ID: 1)**, not by name.
* Adding a column, dropping a column, or renaming a column is an **instant metadata-only operation** taking milliseconds.

---

## 6. Time Travel and Rollbacks

Because Iceberg never deletes historical files immediately, every commit creates an immutable snapshot. This gives you native **Time Travel**:

```sql
-- Query the table as it existed at a specific timestamp
SELECT count(*) 
FROM lakehouse.bronze.oms_orders_hello 
FOR TIMESTAMP AS OF (TIMESTAMP '2026-10-19 10:00:00 UTC');

-- Query the table as of a specific Snapshot ID
SELECT count(*) 
FROM lakehouse.bronze.oms_orders_hello 
FOR VERSION AS OF 837492817492;
```

### Real-World Use Cases:
1. **Reproducible Machine Learning:** Train models on the exact dataset version used 6 months ago.
2. **Instant Rollback:** If a buggy batch job corrupts data at 02:00 AM, roll the catalog pointer back to the 01:59 AM snapshot instantly.
3. **Audit & Compliance:** Prove to financial auditors exactly what numbers were reported at month-end.

---

## 7. Trade-Off Analysis: Iceberg vs. Hive Table Format

| Capability | Legacy Hive Table Format | Apache Iceberg |
| :--- | :--- | :--- |
| **Commit Mechanism** | File system rename / directory listing | Atomic Catalog Pointer Swap |
| **ACID Guarantee** | No (Partial writes leave dirty files) | Full ACID (Snapshot Isolation) |
| **Cloud Storage Speed** | Slow (recursive `O(N)` S3 directory scans) | Sub-second (metadata tree in memory) |
| **Concurrent Writers** | Risky (causes race conditions and data loss) | Safe (Optimistic Concurrency Control) |
| **Partitioning** | Manual (users must query explicit partition columns) | Hidden Partitioning (engine handles transforms) |
| **Time Travel** | Not supported | Fully supported out-of-the-box |
| **Operational Trade-off** | Simple (just drop files in folders) | Requires a Catalog service (e.g., Polaris) |

---

## 8. Practical Connection: Our Olist Project Mapping

In our repository:
* **Storage Location:** All Iceberg data files are written to MinIO bucket `s3://lakehouse/`.
* **Table Identification:** Fully qualified table names use three tiers:
  $$\text{catalog}.\text{namespace}.\text{table\_name}$$
  Example: `lakehouse.bronze.oms_orders_hello`.
* **Spark Configuration:** In [spark_jobs/smoke_test_e2e.py](file:///Users/anhtus/Documents/Development/DE/GRT_DA-DE/spark_jobs/smoke_test_e2e.py), we register Iceberg extensions:
  ```python
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.lakehouse.type", "rest") \
  .config("spark.sql.catalog.lakehouse.uri", "http://polaris:8181")
  ```

---

## 9. Knowledge Check (Self-Assessment)

1. **Question:** *What is the fundamental difference between Apache Parquet and Apache Iceberg?*  
   * **Answer:** Parquet is a **file format** that organizes data into columnar chunks inside a single file. Iceberg is a **table format** that manages collections of Parquet files to provide database-grade ACID transactions, schema evolution, and snapshots across an entire table.

2. **Question:** *How does Iceberg prevent readers (Trino) from seeing partial data when a writer (Spark) fails mid-job?*  
   * **Answer:** Spark writes data to new Parquet files without altering existing files. The catalog pointer is only swapped to the new metadata snapshot after all writes successfully complete. If the job fails, the pointer is never updated, and readers only see the previous committed snapshot.

3. **Question:** *Why does renaming a column in an Iceberg table take 5 milliseconds instead of rewriting 100GB of Parquet files?*  
   * **Answer:** Iceberg tracks columns by immutable integer IDs rather than names. Renaming a column simply updates the name mapped to that column ID in the `v<N>.metadata.json` file. The underlying Parquet data files remain untouched.

---
*Made by Anh Tu - Share to be share*
