---
title: Module 1 — Lakehouse basics
short_title: Module 1
---

# Module 1 (30 min) — Lakehouse basics, Delta Lake, time travel

By the end of this module you will be able to explain the **lakehouse** to
someone who only knows SQL Server + SSIS, run **ACID** operations on a
**Delta** table, and use **time travel** + **`MERGE INTO`** to do things that
would have required an SSIS *Slowly Changing Dimension* wizard.

```{tip}
The full hands-on portion of this module is also packaged as
[`notebooks/01_lakehouse_basics.ipynb`](notebooks/01_lakehouse_basics.ipynb)
for direct upload to Databricks Free Edition.
```

```{contents}
:local:
:depth: 2
```

## 1. Lake vs warehouse vs lakehouse

```{mermaid}
flowchart LR
    subgraph lake [Data Lake]
        f1[CSV]
        f2[JSON]
        f3[Parquet]
        f4[Logs]
    end
    subgraph wh [Data Warehouse]
        t1[Modeled tables]
        t2[Stored procs]
    end
    subgraph lh [Data Lakehouse]
        d1[Delta tables<br/>ACID + governance]
        d2[Open files<br/>Parquet + transaction log]
    end
    lake -. cheap, flexible, but no ACID .-> analyst1[Analyst struggles]
    wh -. ACID, fast SQL, but expensive and schema-on-write .-> analyst2[Analyst happy, ETL team unhappy]
    lh --> analyst3[One platform for ETL + BI + ML]
```

**SSIS parallel.** In your previous BI lab the *warehouse* was SQL Server and
the *lake* was a folder of CSVs your SSIS packages dumped into a shared
drive. The lakehouse collapses both: storage is open files (Parquet) governed
by an ACID transaction log; the same files are queryable as a Delta table
*and* directly readable by Spark, Pandas, or DuckDB.

## 2. Delta Lake = Parquet + a transaction log

A Delta table is just a directory of **Parquet** data files plus a
`_delta_log/` directory of small JSON files describing every commit
(add file, remove file, schema change, …). That log is what gives you:

- **A**tomic, **C**onsistent, **I**solated, **D**urable writes — exactly the
  guarantees you got from a SQL Server transaction.
- **Time travel** (read the table as it was at any past commit).
- **Schema evolution** without rewriting the data files.
- **`MERGE INTO`** (upsert) without juggling staging tables and SSIS *Lookup*
  + *Conditional Split* + *OLE DB Command*.

```{note}
**SSIS parallel — the transaction log.** The Delta `_delta_log/` plays the
same role as the **SQL Server transaction log**: it records every operation
that ever touched the table and is the source of truth for both ACID and
recovery. The big difference is that you can `SELECT` from any past version
with one extra clause — no DBA or backup file required.
```

## 3. Hands-on — ACID on a Delta table

In your `bi_course` notebook (default language SQL), run each block and look
at the output before moving on.

### 3.1 Inspect the table

```sql
USE workspace.bi_course;
DESCRIBE EXTENDED workspace.bi_course.fact_sales;
```

Look for `Provider: delta` and the `Location` row pointing at object storage.
That confirms you're talking to a **managed Delta table**.

### 3.2 An atomic update

Suppose order `1003` was mis-priced and should be 1150 instead of 1100.

```sql
UPDATE workspace.bi_course.fact_sales
SET    revenue = 1150.00
WHERE  order_id = 1003;
```

```{note}
**SSIS parallel.** In SSIS you would build a data flow with an *OLE DB
Source* for the wrong-priced row, a *Derived Column* to fix the price, and an
*OLE DB Command* to issue an `UPDATE`. Or, more realistically, you'd write
this exact `UPDATE` in T-SQL and execute it from an *Execute SQL Task*.
On Delta, the difference is that the update is **ACID even on object
storage** — concurrent readers will not see a half-applied write.
```

### 3.3 An accidental insert

```sql
INSERT INTO workspace.bi_course.fact_sales VALUES
    (9999, DATE'2026-01-21', 99, 99, 1, 0.00);
```

The `product_id = 99` and `region_id = 99` rows do not exist in the
dimensions — this is the classic "broken referential integrity from a flaky
SSIS Lookup" bug.

### 3.4 Audit the table — `DESCRIBE HISTORY`

```sql
DESCRIBE HISTORY workspace.bi_course.fact_sales;
```

You will see one row per commit: the original `CREATE OR REPLACE TABLE`,
the `INSERT` from setup, your `UPDATE`, and your bad `INSERT`. Each row
includes `version`, `timestamp`, `operation`, `operationParameters`, and the
user.

```{tip}
**SSIS parallel.** Closest equivalent in your old stack:
*SSISDB execution log* + parsing the SQL Server *transaction log*. Delta
gives it to you in one query, with row-level operation type, for every
table.
```

### 3.5 Time travel — read a past version

```sql
-- The "good" version, before the bad insert. Replace 2 with whatever
-- version DESCRIBE HISTORY shows just before the bad INSERT.
SELECT COUNT(*) AS rows_at_v2
FROM workspace.bi_course.fact_sales VERSION AS OF 2;
```

You can also time-travel with a timestamp:

```sql
SELECT COUNT(*) AS rows_one_minute_ago
FROM workspace.bi_course.fact_sales TIMESTAMP AS OF current_timestamp() - INTERVAL 1 MINUTE;
```

### 3.6 Roll back the bad insert (without restoring a backup)

Two equally valid ways:

```sql
-- Option A: surgical delete.
DELETE FROM workspace.bi_course.fact_sales WHERE order_id = 9999;

-- Option B: restore the whole table to a known-good version.
RESTORE TABLE workspace.bi_course.fact_sales TO VERSION AS OF 2;
```

```{warning}
`RESTORE` is itself a new commit — it does not erase history. Re-run
`DESCRIBE HISTORY` and you'll see a `RESTORE` row appended on top.
```

## 4. The medallion architecture

The lakehouse equivalent of *staging → transformed → reporting* tables is the
**bronze / silver / gold** medallion:

```{mermaid}
flowchart LR
    raw[Raw source<br/>CSV, JSON, Kafka] --> bronze[Bronze<br/>raw landed Delta]
    bronze --> silver[Silver<br/>cleaned, conformed]
    silver --> gold[Gold<br/>BI-ready aggregates]
    gold --> dash[AI/BI Dashboard]
    gold --> genie[AI/BI Genie]
    gold --> ml[ML / feature store]
```

- **Bronze**: ingest exactly as received, append-only. Equivalent to your SSIS
  *staging* tables — but Delta, so you can re-read history.
- **Silver**: deduplicated, validated, joined to dims. Equivalent to your
  SSIS *transformed* tables.
- **Gold**: business-level aggregates, the things the dashboard and Genie
  query. Equivalent to your SSAS cube or your SSRS report's stored procedure.

For this 2-hour course we collapse the layers (`fact_sales` already plays
Silver, the `v_kpi_*` views in Module 2 play Gold), but everything you build
in Module 2 is a candidate Gold view.

## 5. Stretch — `MERGE INTO` is your SCD wizard

A common BI task: a daily extract of product master data lands as a "delta",
and you must apply it to `dim_product` — update existing rows, insert new
ones. In SSIS you would drag the *Slowly Changing Dimension* wizard and walk
through five screens. On Delta it is one statement.

### 5.1 Build the staging "today's extract"

```sql
CREATE OR REPLACE TEMP VIEW v_product_updates AS
SELECT * FROM VALUES
    (3, 'Wireless Mouse', 'Accessories',  29.00),  -- price bump
    (5, 'Monitor 27"',    'Displays',    310.00),  -- price bump
    (7, 'Webcam HD',      'Accessories',  60.00)   -- new product
AS t(product_id, product_name, category, list_price);
```

### 5.2 Type-1 SCD with `MERGE INTO`

```sql
MERGE INTO workspace.bi_course.dim_product AS tgt
USING v_product_updates                    AS src
   ON tgt.product_id = src.product_id
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM workspace.bi_course.dim_product ORDER BY product_id;
```

You should see two updated rows (3 and 5) and one new row (7).

```{note}
**SSIS parallel — Type 1 SCD wizard, in 6 lines.** This single `MERGE`
replaces:

1. *OLE DB Source* on the staging extract,
2. *Lookup* against `dim_product` on the business key,
3. *Conditional Split* on `Lookup Match Output` vs `No Match Output`,
4. *OLE DB Command* (UPDATE) on the matched branch,
5. *OLE DB Destination* (INSERT) on the unmatched branch.
```

### 5.3 (Optional) Type-2 sketch

For a Type-2 SCD you would extend `dim_product` with `effective_from`,
`effective_to`, and `is_current` columns, then write a `MERGE` that closes
the old row and inserts a new one. Worked example in
[`appendix-ssis-to-databricks.md`](appendix-ssis-to-databricks.md#type-2-scd).

## Free Edition check

- All operations above run on the serverless SQL warehouse — no compute
  configuration needed.
- `DESCRIBE HISTORY` and time-travel queries work on every Delta table you
  own; no special "audit" feature to enable.
- If `RESTORE TABLE` errors with *version not retained*, you have hit the
  default 30-day retention. On Free Edition this never happens during a
  workshop.

## Recap — what you can now claim on your CV

- "I have used Delta Lake on Databricks to do ACID upserts and table-level
  time travel."
- "I can explain the medallion (bronze/silver/gold) architecture and map it
  back to a classic SSIS staging-to-mart flow."
- "I can replace an SSIS Slowly Changing Dimension wizard with a single
  `MERGE INTO`."

On to [Module 2 — BI analysis with SQL and PySpark](module-2-bi-with-sql.md).
