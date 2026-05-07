---
title: Appendix — SSIS ↔ Databricks
short_title: SSIS ↔ Databricks
---

# Appendix — the deep SSIS ↔ Databricks Rosetta stone

This appendix is the migration reference. It is structured as four
independent sections so you can jump straight to whatever you need:

1. [Concepts](#1-concepts) — every SSIS / SSMS / SQL Agent term with the
   lakehouse object that plays the same role.
2. [Transformations](#2-transformations) — side-by-side examples of every
   common SSIS data-flow transformation in **SQL** and **PySpark**.
3. [Migration recipe](#3-migration-recipe) — how to port an existing
   `.dtsx` package to a Databricks notebook + Lakeflow Job in five steps.
4. [Net-new lakehouse capabilities](#4-net-new-lakehouse-capabilities) —
   the bits that have **no** SSIS / SSMS analogue and are worth highlighting
   to your supervisor.

```{contents}
:local:
:depth: 2
```

## 1. Concepts

| SSIS / SSMS / SQL Server world | Databricks lakehouse equivalent |
| --- | --- |
| SQL Server *instance* | A Databricks **workspace** with a serverless SQL warehouse. |
| `master` / user databases | Unity Catalog: **catalog → schema → table** (`workspace.bi_course.fact_sales`). |
| `dbo` schema | Any Unity Catalog **schema** (`bi_course`). |
| SSMS Object Explorer | **Catalog Explorer**. |
| SSMS query window | **SQL Editor** + serverless SQL warehouse. |
| SSISDB (Integration Services Catalog) | **Unity Catalog** as the governed namespace; **Lakeflow Jobs** for the run history. |
| `.dtsx` package | A **notebook** (interactive) or a **Lakeflow Job** task (scheduled). |
| Control Flow | A **Lakeflow Job** with task dependencies. |
| Data Flow | A single PySpark / SQL transformation, *or* a **Lakeflow Spark Declarative Pipeline** for a declarative ETL graph. |
| OLE DB Source | `SELECT … FROM …` / `spark.read.table(...)` / `spark.read.format("csv")`. |
| OLE DB Destination | `INSERT INTO … SELECT …` / `df.write.saveAsTable(name, mode="append")`. |
| Flat File Source / Destination | `spark.read.csv(...)` / `df.write.csv(...)`. |
| Lookup transformation | A SQL `JOIN` or `df.join(other, on, how)`. |
| Derived Column | `CASE WHEN …` / `df.withColumn("c", when(...).otherwise(...))`. |
| Conditional Split | `WHERE` / `df.filter(...)` (or split with two filtered branches). |
| Aggregate | `GROUP BY` / `df.groupBy().agg(...)`. |
| Sort | `ORDER BY` (often replaced with a window function to avoid a global shuffle). |
| Merge / Merge Join | SQL `JOIN` / `df.join(...)`. |
| Multicast | Reuse a Spark DataFrame in multiple downstream branches (Spark caches the lineage; only one read of the source). |
| Union All | SQL `UNION ALL` / `df1.unionByName(df2)`. |
| Pivot / Unpivot | SQL `PIVOT` / `UNPIVOT`, or `df.groupBy(...).pivot("col").agg(...)`. |
| Slowly Changing Dimension wizard | A single `MERGE INTO` against a Delta table. |
| Script Component (row-by-row) | A PySpark **UDF** or, much better, a vectorised Spark expression. |
| Execute SQL Task | A `%sql` cell in a notebook, or a **SQL task** in a Lakeflow Job. |
| Variables / Parameters | Notebook **widgets** + Job parameters. |
| Configurations (XML / SQL) | Notebook widget defaults + Job parameter overrides. |
| Error output / row redirect | Lakeflow Pipeline **expectations** with a quarantine table. |
| SQL Server Agent schedule | **Lakeflow Job** schedule (cron, on serverless compute). |
| `sys.dm_exec_*` DMVs | **Query history** + **Lakeflow run history**. |
| SQL Server transaction log | Delta Lake **transaction log** (`_delta_log/`); enables `DESCRIBE HISTORY` and `VERSION AS OF`. |
| Database backup / restore | Delta **time travel** + `RESTORE TABLE … TO VERSION AS OF n`. |
| SSRS report / Power BI Desktop | **AI/BI Dashboard**. |
| SSAS cube | A **gold-layer view** in Unity Catalog (often combined with materialised views and BI semantic models). |
| *No analogue* | **AI/BI Genie** — natural-language Q&A over a Unity Catalog schema. |

## 2. Transformations

Each subsection below is the **SSIS transformation** you would have dragged
onto the canvas, then the **SQL** version, then the **PySpark DataFrame**
version. They all assume the `bi_course` schema from the workshop.

### 2.1 Lookup

> *SSIS:* OLE DB Source `fact_sales` → Lookup against `dim_product` on
> `product_id` → Derived Column to keep `product_name`.

```sql
SELECT f.*, p.product_name
FROM   fact_sales  f
LEFT JOIN dim_product p USING (product_id);
```

```python
fact    = spark.table("fact_sales")
product = spark.table("dim_product").select("product_id", "product_name")
out     = fact.join(product, on="product_id", how="left")
```

### 2.2 Derived Column

> *SSIS:* Derived Column adds `revenue_band = (revenue >= 1000) ? "high" : "low"`.

```sql
SELECT
    *,
    CASE WHEN revenue >= 1000 THEN 'high' ELSE 'low' END AS revenue_band
FROM   fact_sales;
```

```python
out = (
    spark.table("fact_sales")
         .withColumn(
             "revenue_band",
             F.when(F.col("revenue") >= 1000, "high").otherwise("low"),
         )
)
```

### 2.3 Conditional Split

> *SSIS:* split a single input into two outputs by a predicate.

```sql
-- branch A
SELECT * FROM fact_sales WHERE revenue >= 1000;
-- branch B
SELECT * FROM fact_sales WHERE revenue <  1000;
```

```python
big   = spark.table("fact_sales").filter(F.col("revenue") >= 1000)
small = spark.table("fact_sales").filter(F.col("revenue") <  1000)
```

### 2.4 Aggregate

> *SSIS:* `SUM(revenue)` grouped by `region_id`.

```sql
SELECT region_id, SUM(revenue) AS revenue
FROM   fact_sales
GROUP BY region_id;
```

```python
agg = (
    spark.table("fact_sales")
         .groupBy("region_id")
         .agg(F.sum("revenue").alias("revenue"))
)
```

### 2.5 Sort

> *SSIS:* sort by `order_date` ascending.

```sql
SELECT * FROM fact_sales ORDER BY order_date;
```

```python
sorted_df = spark.table("fact_sales").orderBy("order_date")
```

### 2.6 Merge Join

> *SSIS:* full-outer merge join of `fact_sales` and an external order-status
> stream on `order_id`.

```sql
SELECT *
FROM   fact_sales        f
FULL OUTER JOIN order_status s USING (order_id);
```

```python
out = fact.join(status, on="order_id", how="full_outer")
```

### 2.7 Slowly Changing Dimension — Type 1

> *SSIS:* SCD wizard, "Changing attribute" mode (overwrite).

```sql
MERGE INTO dim_product AS tgt
USING v_product_updates AS src
   ON tgt.product_id = src.product_id
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

```python
from delta.tables import DeltaTable
tgt = DeltaTable.forName(spark, "workspace.bi_course.dim_product")
(
    tgt.alias("t")
       .merge(spark.table("v_product_updates").alias("s"), "t.product_id = s.product_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
)
```

(#type-2-scd)=
### 2.8 Slowly Changing Dimension — Type 2

> *SSIS:* SCD wizard, "Historical attribute" mode. Adds `effective_from`,
> `effective_to`, `is_current` columns and emits two rows on a change.

```sql
-- One-time schema bump on dim_product (run once).
ALTER TABLE dim_product
    ADD COLUMNS (
        effective_from TIMESTAMP,
        effective_to   TIMESTAMP,
        is_current     BOOLEAN
    );

UPDATE dim_product
SET    effective_from = current_timestamp(),
       is_current     = TRUE
WHERE  effective_from IS NULL;

-- Now the SCD-2 MERGE itself. Two passes:
--   pass 1: close the matching current row when a tracked attribute changes;
--   pass 2: insert the new current row plus brand-new products.
MERGE INTO dim_product AS tgt
USING (
    SELECT s.*, NULL AS effective_to_close
    FROM   v_product_updates s
    JOIN   dim_product        d
      ON   d.product_id = s.product_id
     AND   d.is_current = TRUE
     AND   d.list_price <> s.list_price
) AS upd
   ON tgt.product_id = upd.product_id AND tgt.is_current = TRUE
WHEN MATCHED THEN UPDATE SET
    is_current     = FALSE,
    effective_to   = current_timestamp();

INSERT INTO dim_product (product_id, product_name, category, list_price,
                         effective_from, effective_to, is_current)
SELECT s.product_id, s.product_name, s.category, s.list_price,
       current_timestamp(), NULL, TRUE
FROM   v_product_updates s
LEFT   JOIN dim_product d
       ON d.product_id = s.product_id AND d.is_current = TRUE
WHERE  d.product_id IS NULL          -- brand new product
   OR  d.list_price <> s.list_price; -- attribute changed
```

The PySpark equivalent uses `DeltaTable.merge(...)` twice with the same
predicates; the SQL version above is easier to read in a workshop setting.

### 2.9 Multicast

> *SSIS:* duplicate a single source to N parallel branches.

```python
fact = spark.table("fact_sales").cache()  # explicit cache is rarely needed
branch_a = fact.groupBy("region_id").agg(F.sum("revenue"))
branch_b = fact.groupBy("product_id").agg(F.sum("quantity"))
```

In SQL you simply write the two queries; the warehouse's optimiser shares
scans where it can.

### 2.10 Union All

```sql
SELECT * FROM fact_sales_2025
UNION ALL
SELECT * FROM fact_sales_2026;
```

```python
out = df_2025.unionByName(df_2026)
```

## 3. Migration recipe

Given an existing `.dtsx` package, port it to Databricks in five steps:

1. **Open the package in Visual Studio** (SSDT). Note the package's *Control
   Flow* tasks in order — these become the **Lakeflow Job task graph**.
2. **For each Data Flow Task**, list its sources, transformations, and
   destinations. Each Data Flow becomes either:
   - a single `%sql` cell (preferred when the logic is set-based), or
   - a single PySpark cell (when you need fine-grained imperative control).
3. **Translate sources & destinations** using §1 and §2: OLE DB Source ↔
   `spark.read.table` / `SELECT`; OLE DB Destination ↔ `df.write.saveAsTable`
   / `INSERT INTO`. Flat File sources become `spark.read.csv(path)`.
4. **Translate transformations** using §2. Most SSIS transformations
   collapse into 1–2 lines of SQL or PySpark; the worst offenders (Script
   Components, Slowly Changing Dimension) collapse into one `MERGE INTO`.
5. **Wrap the resulting notebook(s) in a Lakeflow Job.** Recreate the
   *Control Flow* dependencies (`On Success` / `On Failure`) as task
   dependencies in the Job graph, and recreate the *SQL Server Agent*
   schedule as a Job schedule.

```{tip}
If your package was full of *Execute SQL Tasks* on a SQL Server backend, the
fastest first cut is: **one notebook of `%sql` cells**, run end-to-end on
the serverless warehouse, then promote stable steps into separate Lakeflow
Job tasks.
```

## 4. Net-new lakehouse capabilities

These have **no SSIS / SSMS equivalent**. They are the strongest selling
points to a stakeholder who already has SSIS working "well enough":

- **Time travel** (`SELECT … VERSION AS OF n` / `TIMESTAMP AS OF t`).
  Every Delta table is its own backup, queryable in place.
- **Schema evolution** without rewriting Parquet files. Add a column to a
  Delta table; readers see `NULL` for old rows; no SSIS data-flow rewrite.
- **`MERGE INTO`** as a first-class single-statement upsert.
- **Unity Catalog lineage**, including column-level lineage, with no extra
  agent or scanner installed.
- **AI/BI Genie** — natural-language Q&A over a governed Unity Catalog
  schema. There is no SSIS or SSRS equivalent; the closest historical
  product was Power BI Q&A, but Genie is built into the same governance
  layer as the data.
- **Spark Declarative Pipelines** (formerly Delta Live Tables) — declare
  the dependency graph of bronze/silver/gold tables; the platform handles
  orchestration, expectations, retries, and incrementalisation.

These are the items worth leading with when you write up a "should we move
off SSIS?" memo.
