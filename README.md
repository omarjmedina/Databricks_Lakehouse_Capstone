# Retail Analytics Lakehouse Pipeline on Databricks

## Overview

This capstone project demonstrates the implementation of a complete data pipeline on the Databricks Lakehouse Platform. The solution integrates Lakehouse architecture, Spark DataFrame transformations, Delta Lake transactional storage, and Databricks workflow orchestration to process retail sales transactions from multiple regional systems.

The project is divided into three parts:

* **Part 1:** Lakehouse Architecture, Unity Catalog, and Compute Resources
* **Part 2:** Notebook Development and Spark DataFrame Operations
* **Part 3:** Delta Lake Management, MERGE Operations, Time Travel, and Workflow Automation

---

## Project Scenario

As a Data Engineer at a retail analytics company, the objective is to build a scalable Lakehouse pipeline that processes daily sales transaction data available within the Databricks catalog.

The pipeline must:

* Ingest source data into a Raw Delta table
* Clean and transform records into a Curated Delta table
* Generate business summary metrics
* Handle incremental updates using Delta MERGE operations
* Automate execution through Databricks Jobs

---

## Architecture

```text
Source Tables
      │
      ▼
Raw Delta Layer
      │
      ▼
Curated Delta Layer
      │
      ▼
Summary Delta Layer
      │
      ▼
Analytics & Reporting
```

### Workflow Pipeline

```text
Ingest Data
      │
      ▼
Clean & Transform
      │
      ▼
Aggregate Metrics
      │
      ▼
MERGE Incremental Updates
      │
      ▼
Dashboard Refresh
```

---

## Technologies Used

* Databricks Lakehouse Platform
* Apache Spark (PySpark)
* Delta Lake
* Unity Catalog
* Databricks Workflows
* SQL
* Python

---

## Repository Structure

```text
.
├── labs/
│   ├── Part1/
│   │   ├── lab_lakehouse.ipynb
│   │   └── lab_workspace.ipynb
│   │
│   ├── Part2/
│   │   ├── lab_notebooks.ipynb
│   │   └── lab_spark.ipynb
│   │
│   └── Part3/
│       ├── lab_delta.ipynb
│       └── lab_workflows.ipynb
│
├── README.md
```

---

## Part 1: Lakehouse Architecture & Unity Catalog

### Lakehouse Concepts

Skills Applied:

* Compare Data Warehouse, Data Lake, and Lakehouse architectures
* Understand Delta Lake as the storage layer
* Verify Spark runtime environment
* Explore Databricks workspace features

### Workspace & Catalog

Skills Applied:

* Navigate Unity Catalog hierarchy (Catalog → Schema → Table)
* Explore DBFS storage
* Inspect cluster and compute resources
* Manage data assets within Databricks

---

## Part 2: Spark Data Processing

### Using Notebooks

Skills Applied:

* Magic commands (`%sql`, `%python`, `%run`)
* `dbutils` file operations
* Notebook organization and execution
* Data visualization using `display()`

### Spark DataFrame Operations

Core transformations used throughout the project:

```python
select()
filter()
withColumn()
groupBy()
agg()
join()
```

Example transformation:

```python
from pyspark.sql.functions import col

curated_df = (
    sales_df
        .filter(col("sales_amount") > 0)
        .withColumn(
            "revenue",
            col("quantity") * col("unit_price")
        )
)
```

---

## Part 3: Delta Lake Management

### Raw Delta Table

The source sales data is ingested into a Delta table while preserving the original schema.

```python
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.raw_sales")
```

Validation:

```sql
DESCRIBE DETAIL retail.raw_sales;
```

Features:

* Schema enforcement
* ACID transactions
* Delta transaction log

---

### Curated Delta Table

Data cleansing operations include:

* Removing invalid records
* Standardizing fields
* Creating derived business columns
* Preparing data for analytics

Example derived field:

```python
.withColumn(
    "revenue",
    col("quantity") * col("unit_price")
)
```

---

### Summary Delta Tables

Business metrics are generated using Spark aggregations.

```python
summary_df = (
    curated_df
        .groupBy("region", "category")
        .agg(
            sum("revenue").alias("total_revenue")
        )
)
```

Metrics Produced:

* Revenue by Region
* Revenue by Product Category
* Daily Revenue Totals

---

## Incremental Processing with MERGE

To handle duplicate records and upstream retries, Delta Lake MERGE operations are used.

```sql
MERGE INTO retail.curated_sales AS target
USING retail.sales_updates AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED THEN
UPDATE SET *

WHEN NOT MATCHED THEN
INSERT *
```

Benefits:

* Supports inserts and updates
* Eliminates duplicate records
* Maintains ACID compliance
* Executes as a single atomic transaction

---

## Delta Time Travel & Recovery

### View Table History

```sql
DESCRIBE HISTORY retail.curated_sales;
```

### Query Previous Versions

```sql
SELECT *
FROM retail.curated_sales
VERSION AS OF 2;
```

### Restore Previous Version

```sql
RESTORE TABLE retail.curated_sales
TO VERSION AS OF 2;
```

### Recovery Runbook

1. Run `DESCRIBE HISTORY` to review changes.
2. Identify the desired version.
3. Validate data using `VERSION AS OF`.
4. Execute `RESTORE`.
5. Verify data integrity after recovery.

The Delta transaction log is append-only, ensuring complete auditability of all changes.

---

## Workflow Automation

The pipeline is designed as a multi-task Databricks Job.

### Task Dependencies

```text
Task 1: Ingest Data
        ↓
Task 2: Clean Data
        ↓
Task 3: Aggregate Metrics
        ↓
Task 4: Dashboard Refresh
```

## Deliverables

### Raw Delta Table

* Source data loaded into Delta format
* Schema enforcement enabled
* Queryable through Unity Catalog

### Curated Delta Table

* Invalid records removed
* Derived fields created
* Analytics-ready dataset

### Summary Delta Tables

* Revenue by region
* Revenue by category
* Daily sales metrics

### MERGE Pipeline

* Incremental upsert processing
* Duplicate prevention
* Update and insert support

### Recovery Runbook

* Audit history procedures
* Time travel validation
* Table restoration process

### Workflow Plan

* Multi-task Databricks Job
* Scheduled execution
* Task dependency management

## Learning Outcomes

This project demonstrates proficiency in:

* Lakehouse Architecture
* Unity Catalog Management
* Databricks Notebook Development
* Spark DataFrame Transformations
* Delta Lake Operations
* MERGE-Based Incremental Processing
* Delta Time Travel
* Workflow Automation
* End-to-End Data Engineering Design