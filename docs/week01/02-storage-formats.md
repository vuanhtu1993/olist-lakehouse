# Module 02: Storage Formats for Analytics — Parquet vs. CSV Deep Dive

> **Target Audience:** IT Students & Software Engineers transitioning to Data Engineering  
> **Topic Scope:** Row-oriented vs. Columnar Storage, Apache Parquet Internals, Compression, Predicate Pushdown, Trade-offs  
> **Estimated Reading Time:** 15 minutes  

---

## 1. The Big Picture: Why Text Files Fail at Scale

In software development, CSV (Comma-Separated Values) and JSON are the default choices for data exchange. They are human-readable, simple to parse, and supported by every programming language.

However, when processing millions or billions of rows in an analytical system (OLAP), plain-text files create massive performance bottlenecks:

1. **Massive Disk I/O:** Text files store numbers, dates, and booleans as strings of ASCII characters. Storing the number `123456789` takes 9 bytes in CSV, but only 4 bytes as an integer in binary formats.
2. **Slow Parsing Overhead:** Every time Spark reads a CSV, CPU threads spend heavy processing power scanning characters, escaping quotes, and converting strings back into integers and timestamps.
3. **Full Table Scans:** To calculate the sum of just *one* column, the storage engine must read the *entire* file from disk into memory.

To solve this, modern Data Lakehouses use **binary, columnar file formats**, primarily **Apache Parquet**.

```mermaid
flowchart TD
    subgraph RowOriented ["1. Row-Oriented Storage (CSV / PostgreSQL Heap)"]
        direction TB
        R1["Row 1: [order_id: 1, customer: 'Alice', amount: 150.0, status: 'delivered']"]
        R2["Row 2: [order_id: 2, customer: 'Bob',   amount: 80.0,  status: 'shipped']"]
        R3["Row 3: [order_id: 3, customer: 'Carol', amount: 210.0, status: 'delivered']"]
        R1 --- R2 --- R3
        R_Note["To read just 'amount', the engine must scan ALL 4 columns across all rows from disk."]
    end

    subgraph ColumnOriented ["2. Column-Oriented Storage (Apache Parquet)"]
        direction TB
        C1["Column 'order_id': [1, 2, 3]"]
        C2["Column 'customer': ['Alice', 'Bob', 'Carol']"]
        C3["Column 'amount':   [150.0, 80.0, 210.0]"]
        C4["Column 'status':   ['delivered', 'shipped', 'delivered']"]
        C1 --- C2 --- C3 --- C4
        C_Note["To calculate SUM(amount), the engine reads ONLY Column 'amount' from disk. 75% I/O skipped!"]
    end

    classDef rowStyle fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b;
    classDef colStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;
    classDef noteStyle fill:#f8fafc,stroke:#94a3b8,stroke-width:1px,color:#334155;

    class R1,R2,R3 rowStyle;
    class C1,C2,C3,C4 colStyle;
    class R_Note,C_Note noteStyle;
```

---

## 2. Concept Anatomy: Apache Parquet

### Academic Definition
> **Apache Parquet** is an *open-source, column-oriented binary data file format* designed for efficient data storage and retrieval, providing complex nested data structures, efficient compression encodings, and rich metadata statistics per column chunk.

### Keyword Dissection (Anatomy)

```mermaid
flowchart TD
    Root["Apache Parquet Core Anatomy"]

    subgraph StorageLayout ["1. Columnar Layout"]
        L1["Row Groups: Horizontal partitions of data (typically 128MB - 512MB)"]
        L2["Column Chunks: Contiguous blocks of data for a single column"]
        L3["Data Pages: Smallest unit of encoding and compression (typically 1MB)"]
    end

    subgraph MetadataLayer ["2. Self-Describing Metadata (File Footer)"]
        M1["Thrift Schema: Explicit column names, data types, and nullability"]
        M2["Column Statistics: Min/Max values, Null count, Total row count per chunk"]
    end

    subgraph EncodingEngine ["3. Efficient Encodings"]
        E1["Dictionary Encoding: Replace repeated strings with integer IDs"]
        E2["Run-Length Encoding (RLE) & Bit Packing"]
        E3["Snappy / Zstandard (ZSTD) Compression Algorithms"]
    end

    Root --> StorageLayout
    Root --> MetadataLayer
    Root --> EncodingEngine

    classDef rootNode fill:#1e293b,stroke:#0f172a,stroke-width:2px,color:#ffffff;
    classDef layoutGroup fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef metaGroup fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#b45309;
    classDef encodeGroup fill:#f0fdf4,stroke:#16a34a,stroke-width:2px,color:#15803d;

    class Root rootNode;
    class L1,L2,L3 layoutGroup;
    class M1,M2 metaGroup;
    class E1,E2,E3 encodeGroup;
```

---

## 3. Physical File Structure of Parquet

A Parquet file is divided into three key physical sections: **Header**, **Row Groups**, and **File Footer**.

```mermaid
flowchart TB
    Header["PAR1 (4-byte Magic Number)"]

    subgraph RG1 ["Row Group 0 (e.g., Rows 1 to 100,000)"]
        direction LR
        RG1_Col1["Column 1 Chunk<br/>(order_id)"]
        RG1_Col2["Column 2 Chunk<br/>(customer_id)"]
        RG1_Col3["Column 3 Chunk<br/>(freight_value)"]
    end

    subgraph RG2 ["Row Group 1 (e.g., Rows 100,001 to 200,000)"]
        direction LR
        RG2_Col1["Column 1 Chunk<br/>(order_id)"]
        RG2_Col2["Column 2 Chunk<br/>(customer_id)"]
        RG2_Col3["Column 3 Chunk<br/>(freight_value)"]
    end

    subgraph Footer ["File Footer (Read First by Query Engines!)"]
        direction TB
        F_Schema["Table Schema: Data types, column names"]
        F_Stats["Column Statistics per Row Group:<br/>• Column 1: Min = 1, Max = 200000<br/>• Column 3: Min = 5.20, Max = 148.50"]
        F_Length["Footer Length (4 bytes)"]
        F_Magic["PAR1 (4-byte Magic Number)"]
    end

    Header --> RG1 --> RG2 --> Footer

    classDef magicStyle fill:#f1f5f9,stroke:#64748b,stroke-width:1px,color:#334155;
    classDef chunkStyle fill:#e0f2fe,stroke:#0284c7,stroke-width:2px,color:#0369a1;
    classDef footerStyle fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e;

    class Header,F_Magic magicStyle;
    class RG1_Col1,RG1_Col2,RG1_Col3,RG2_Col1,RG2_Col2,RG2_Col3 chunkStyle;
    class F_Schema,F_Stats,F_Length footerStyle;
```

> **Why is the Metadata in the Footer?**  
> In streaming or distributed writing, the total number of rows and column statistics are not known until the file write finishes. By placing metadata in the **Footer**, the writer does not need to buffer the entire file in RAM; it writes data blocks sequentially and appends the metadata at the very end.

---

## 4. The Three Pillars of Parquet Superiority

Why does Parquet query 10x-100x faster than CSV? It comes down to three technical capabilities:

### Pillar 1: Column Projection (I/O Pruning)

In analytical queries, you rarely select every column. Consider this query:
```sql
SELECT status, COUNT(*) FROM orders GROUP BY status;
```
* **CSV:** Must read the entire file from disk (all columns), splitting every line into tokens.
* **Parquet:** The storage engine reads **only the byte offset containing the `status` column chunk**. All other columns (customer name, timestamps, IDs) are completely ignored at the disk level.

### Pillar 2: High Compression Ratio via Homogeneous Typing

In CSV, every row mixes different data types together: `1, "Alice", 150.00, true`. Compression algorithms struggle because adjacent data has high entropy (randomness).

In Parquet, values of the *same* column are stored contiguously:
* A column of Brazilian state codes: `["SP", "SP", "SP", "RJ", "SP", "MG"]`.
* **Dictionary Encoding:** Replaces `"SP"` with `0`, `"RJ"` with `1`, `"MG"` with `2`. The text is stored once in a dictionary, and the data is stored as tiny integers `[0, 0, 0, 1, 0, 2]`.
* **Snappy / Zstandard (ZSTD):** Compresses identical data types 4x to 8x better than gzipped CSV.

### Pillar 3: Predicate Pushdown (Data Skipping)

When you run a filter query:
```sql
SELECT * FROM orders WHERE freight_value > 200.00;
```
1. Before scanning any data blocks, Spark reads the **File Footer**.
2. It inspects the statistics for `Row Group 0`:
   * `freight_value: Min = 5.20, Max = 148.50`.
3. Because the maximum value in `Row Group 0` is `148.50`, it is mathematically impossible for any row in this group to match `> 200.00`.
4. **Spark skips the entire Row Group (100,000 rows) without decompressing or reading a single byte!**

```mermaid
flowchart TD
    Query["Query: WHERE freight_value > 200.00"] --> ReadFooter["1. Read Parquet Footer Metadata"]

    ReadFooter --> CheckRG0{"Row Group 0<br/>Max freight = 148.50<br/>Is Max >= 200.00?"}
    ReadFooter --> CheckRG1{"Row Group 1<br/>Max freight = 310.00<br/>Is Max >= 200.00?"}

    CheckRG0 -->|No| SkipRG0["SKIP Row Group 0<br/>(Zero disk reads)"]
    CheckRG1 -->|Yes| ScanRG1["READ Row Group 1<br/>(Scan Column Chunk)"]

    classDef queryStyle fill:#e0e7ff,stroke:#4338ca,stroke-width:2px,color:#312e81;
    classDef checkStyle fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#92400e;
    classDef skipStyle fill:#fee2e2,stroke:#ef4444,stroke-width:2px,color:#991b1b;
    classDef scanStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;

    class Query,ReadFooter queryStyle;
    class CheckRG0,CheckRG1 checkStyle;
    class SkipRG0 skipStyle;
    class ScanRG1 scanStyle;
```

---

## 5. Technical Comparison: CSV vs. Parquet

| Feature | CSV (Comma-Separated Values) | Apache Parquet |
| :--- | :--- | :--- |
| **Storage Layout** | Row-oriented, plain text | Column-oriented, binary |
| **File Size** | Large (uncompressed or gzip) | Very small (Snappy/ZSTD + Dictionary encoding) |
| **Schema Definition** | Weak (no data types, implicit header) | Strong (Thrift binary schema embedded in file) |
| **Column Projection** | No (must scan all columns) | Yes (reads only requested column chunks) |
| **Data Skipping (Min/Max)** | No (must scan all rows) | Yes (skips entire row groups via footer stats) |
| **Human Readability** | Yes (can open in Notepad / Excel) | No (requires tools like Python, Spark, DuckDB) |
| **Write Speed** | Fast (simple string appending) | Slower (requires encoding, statistics, compression) |
| **Best Used For** | Initial raw data drops, small exports | Analytical data layers (Bronze, Silver, Gold) |

---

## 6. Trade-Off Analysis: Write Latency vs. Read Performance

Every format choice represents an engineering trade-off:

```mermaid
flowchart TB
    subgraph ReadHeavy ["Analytical Workloads (OLAP & Lakehouse)"]
        direction TB
        P_Opt["Apache Parquet<br/><b>Optimized For:</b> High read throughput, complex aggregations, BI queries"]
        P_Trade["<b>Trade-off:</b> High CPU and memory overhead during write time<br/>(calculating statistics, encoding dictionaries, compressing blocks)"]
    end

    subgraph WriteHeavy ["Transactional / Ingestion Workloads (OLTP & Landing)"]
        direction TB
        C_Opt["CSV / JSON Lines<br/><b>Optimized For:</b> Instant write speed, append-only logs, external vendor exchange"]
        C_Trade["<b>Trade-off:</b> Disaster for analytics queries (high storage cost, CPU parsing bottleneck)"]
    end

    classDef parqStyle fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#166534;
    classDef csvStyle fill:#fee2e2,stroke:#dc2626,stroke-width:2px,color:#991b1b;

    class P_Opt,P_Trade parqStyle;
    class C_Opt,C_Trade csvStyle;
```

### Engineering Rule of Thumb:
* If data is written once and read thousands of times by analysts and machine learning models $\rightarrow$ **Always use Parquet**.
* If data is an external vendor drop or a configuration file $\rightarrow$ **Accept CSV at landing, then convert immediately to Parquet in Bronze**.

---

## 7. Practical Connection: Our Olist Project Mapping

Here is how storage formats are handled across our Lakehouse architecture:

| Pipeline Stage | Storage Location | Format | Reason |
| :--- | :--- | :--- | :--- |
| **Source Data** | `data_prep/raw/*.csv` | CSV | Kaggle public dataset is distributed in raw CSV. |
| **Landing Reference** | MinIO `s3://lakehouse/landing/ref/` | CSV | Dimension lookups (`category_translation.csv`) are kept as reference files. |
| **Bronze Layer** | MinIO `s3://lakehouse/bronze/` | **Parquet (Iceberg)** | Converted to Parquet via Spark to enable ACID, compression, and sub-second Trino queries. |
| **Silver & Gold** | MinIO `s3://lakehouse/silver/` & `gold/` | **Parquet (Iceberg)** | High-speed aggregated reads for Superset dashboards. |

In [spark_jobs/smoke_test_e2e.py](file:///Users/anhtus/Documents/Development/DE/GRT_DA-DE/spark_jobs/smoke_test_e2e.py), we explicitly instruct Iceberg to write in Parquet:
```python
df_orders.writeTo("lakehouse.bronze.oms_orders_hello") \
    .tableProperty("write.format.default", "parquet") \
    .createOrReplace()
```

---

## 8. Knowledge Check (Self-Assessment)

1. **Question:** *Why does reading a Parquet file start by reading the end of the file (Footer) rather than the beginning?*  
   * **Answer:** The file footer contains the complete table schema and column statistics (Min/Max values) for every Row Group. By reading the footer first, the query engine can plan which Row Groups to skip before reading any raw data blocks.

2. **Question:** *If a table has 100 columns and a query selects only 2 columns, what is the approximate I/O reduction when using Parquet instead of CSV?*  
   * **Answer:** Approximately 98% disk I/O reduction. Parquet uses columnar projection to read only the contiguous byte offsets for those 2 columns, whereas CSV must read all 100 columns from disk.

3. **Question:** *Why does Parquet achieve higher compression ratios than compressing a CSV file with GZIP?*  
   * **Answer:** Parquet stores values of the same column (same data type) adjacent to each other. Homogeneous data has low entropy and enables specialized algorithms like Dictionary Encoding and Run-Length Encoding before applying general compression algorithms (Snappy/ZSTD).

---
*Made by Anh Tu - Share to be share*
