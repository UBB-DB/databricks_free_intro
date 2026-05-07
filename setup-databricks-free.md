---
title: Setup — Databricks Free Edition
short_title: Setup
---

# Setup — Databricks Free Edition

This page gets every student onto the same starting line in **15 minutes**:
account, workspace tour, a running serverless SQL warehouse, a notebook, and
the three-table sample dataset that powers Modules 1–3.

```{tip}
Skip the typing: a ready-made Jupyter notebook with every cell on this page
is provided at [`notebooks/00_setup.ipynb`](notebooks/00_setup.ipynb). After
step 4 below, you can import it via **Workspace → Import → URL** and run it
top-to-bottom instead of copy-pasting from this page. See
[`notebooks/README.md`](notebooks/README.md) for the three import options.
```

## 1. Create a Free Edition account

1. Open <https://www.databricks.com/learn/free-edition>.
2. Click **Sign up for Free Edition** and register with your university or
   personal email (no credit card needed).
3. Confirm your email, accept the terms, and you will land directly inside
   your personal workspace.

```{tip}
Free Edition is **single-account** — there is no shared workspace and no other
users to collaborate with live. We will lean into individual exercises and
treat the multi-user features (`GRANT`, sharing) as read-only walkthroughs.
```

## 2. Workspace tour — the SSMS you already know, rearranged

The left rail of the workspace is the equivalent of SSMS Object Explorer + a
few extra panes. Map the icons to what you used to use:

- **Workspace** → folders for your notebooks (think *Solution Explorer* in
  Visual Studio for SSIS projects).
- **Catalog** → **Unity Catalog Explorer**. This is the lakehouse equivalent
  of *SSMS Object Explorer*. Everything is namespaced as
  `<catalog>.<schema>.<table>`, e.g. `workspace.bi_course.fact_sales`.
- **SQL Editor** → the equivalent of an *SSMS query window*, attached to a
  serverless SQL warehouse.
- **Compute** → SQL warehouses (for SQL Editor + Dashboards) and serverless
  notebook compute (for Python / PySpark).
- **Jobs & Pipelines** (Lakeflow) → the equivalent of *SQL Server Agent +
  SSIS Catalog scheduling*.
- **Dashboards** → AI/BI Dashboards (the SSRS / Power BI Desktop equivalent).
- **Genie** → AI/BI Genie, natural-language Q&A. There is no SSIS / SSRS
  analogue; this is genuinely new.

## 3. Start a serverless SQL warehouse

You only need one warehouse for the whole workshop.

1. Go to **SQL Warehouses** (under *Compute* or in the left rail).
2. Free Edition gives you a **Serverless Starter Warehouse** by default —
   click **Start**.
3. Wait until the status badge turns green (cold-start is ~30–60 s; this is
   the only "make a coffee" moment of the session).

```{important}
Leave the warehouse running for the whole 2 hours. Every SQL cell, every
dashboard tile, and every Genie question hits this warehouse, so re-starting
it adds 30–60 s of cold-start latency on top of every action.
```

## 4. Create your course notebook

1. **Workspace → +Add → Notebook**, name it `bi_course`.
2. At the top right, set the **default language** to *SQL* and attach
   *Serverless* compute. You can override the language per-cell with
   `%python` / `%sql` magics, exactly like you would mix T-SQL and PowerShell
   in an SSIS project.
3. Add two cells: one `%sql` and one `%python` — you'll need both later.

## 5. Create the course schema in Unity Catalog

This is the lakehouse equivalent of running `CREATE DATABASE bi_course;` on
SQL Server, except the namespace is governed centrally by Unity Catalog.

```sql
-- Free Edition gives every account a default catalog called `workspace`.
CREATE SCHEMA IF NOT EXISTS workspace.bi_course
COMMENT 'BI on the Lakehouse - 2-hour crash course (UBB Cluj)';

USE workspace.bi_course;
```

```{seealso}
Three-part naming (`workspace.bi_course.fact_sales`) is the *only* way to
reference an object on a Unity-Catalog-enabled workspace. There is no
schema-only fallback like `dbo.fact_sales`. Get used to it now — every
example in Modules 2 and 3 assumes it.
```

## 6. Create the reproducible 3-table sample dataset

We deliberately use a **star schema** (one fact, two dimensions) instead of a
single flat table, so that Module 2 can show real `JOIN` / `Lookup` queries
and Module 1 can demonstrate `MERGE INTO` against `dim_product`.

Run all three blocks below in your `%sql` cell.

### `dim_product`

```sql
CREATE OR REPLACE TABLE workspace.bi_course.dim_product (
    product_id   INT      NOT NULL,
    product_name STRING   NOT NULL,
    category     STRING   NOT NULL,
    list_price   DECIMAL(10, 2) NOT NULL
) USING DELTA;

INSERT INTO workspace.bi_course.dim_product VALUES
    (1, 'Laptop 14"',   'Computers',   1200.00),
    (2, 'Laptop 16"',   'Computers',   1800.00),
    (3, 'Wireless Mouse','Accessories',  25.00),
    (4, 'Mech. Keyboard','Accessories',  80.00),
    (5, 'Monitor 27"',  'Displays',     300.00),
    (6, 'Monitor 32"',  'Displays',     520.00);
```

### `dim_region`

```sql
CREATE OR REPLACE TABLE workspace.bi_course.dim_region (
    region_id   INT    NOT NULL,
    region_name STRING NOT NULL,
    country     STRING NOT NULL
) USING DELTA;

INSERT INTO workspace.bi_course.dim_region VALUES
    (1, 'North', 'Romania'),
    (2, 'South', 'Romania'),
    (3, 'East',  'Romania'),
    (4, 'West',  'Romania');
```

### `fact_sales`

```sql
CREATE OR REPLACE TABLE workspace.bi_course.fact_sales (
    order_id    BIGINT  NOT NULL,
    order_date  DATE    NOT NULL,
    product_id  INT     NOT NULL,
    region_id   INT     NOT NULL,
    quantity    INT     NOT NULL,
    revenue     DECIMAL(12, 2) NOT NULL
) USING DELTA;

INSERT INTO workspace.bi_course.fact_sales VALUES
    (1001, DATE'2026-01-15', 1, 1, 1, 1200.00),
    (1002, DATE'2026-01-15', 3, 1, 2,   50.00),
    (1003, DATE'2026-01-16', 1, 2, 1, 1100.00),
    (1004, DATE'2026-01-16', 4, 2, 1,   80.00),
    (1005, DATE'2026-01-17', 5, 4, 1,  300.00),
    (1006, DATE'2026-01-17', 3, 4, 1,   25.00),
    (1007, DATE'2026-01-18', 2, 3, 1, 1800.00),
    (1008, DATE'2026-01-18', 6, 3, 1,  520.00),
    (1009, DATE'2026-01-19', 1, 1, 2, 2400.00),
    (1010, DATE'2026-01-19', 5, 2, 1,  300.00),
    (1011, DATE'2026-01-20', 4, 4, 3,  240.00),
    (1012, DATE'2026-01-20', 2, 3, 1, 1750.00);
```

### Smoke test

```sql
SELECT COUNT(*) AS rows_in_fact FROM workspace.bi_course.fact_sales;
-- Expected: 12

SELECT
    f.order_id,
    p.product_name,
    r.region_name,
    f.revenue
FROM workspace.bi_course.fact_sales       f
JOIN workspace.bi_course.dim_product      p USING (product_id)
JOIN workspace.bi_course.dim_region       r USING (region_id)
ORDER BY f.order_id
LIMIT 5;
```

If the second query returns five neatly joined rows you are ready for
[Module 1](module-1-lakehouse-basics.md).

## SSIS parallel — what just happened

What you did in the last 15 minutes, mapped to your SSIS world:

- Creating the `bi_course` schema = `CREATE DATABASE bi_course` on SQL Server,
  but the database lives in **Unity Catalog** (think SSISDB) instead of an
  on-prem SQL instance.
- Creating the three Delta tables = three `CREATE TABLE` statements on SQL
  Server — except the storage engine is **Delta Lake** (Parquet + transaction
  log), so you get ACID, time travel and schema evolution for free.
- Loading rows with `INSERT … VALUES` = the equivalent of an *OLE DB
  Destination* in an SSIS data flow, with a one-row-per-`VALUES`-tuple
  pattern.
- The smoke-test `JOIN` = three *OLE DB Source* + two *Lookup* transformations
  collapsed into a single SQL statement that the warehouse can plan, push
  down, and parallelise for you.

## Free Edition check

- Warehouse green and idle? Good — every SQL cell runs against it.
- Schema visible in **Catalog Explorer** under `workspace`? Good.
- All three tables show **12 / 6 / 4** rows in the *Sample Data* tab? Good —
  on to Module 1.
