---
title: Module 2 — BI with SQL & PySpark
short_title: Module 2
---

# Module 2 (40 min) — BI analysis with SQL and PySpark

This is the meat of the workshop. We answer four progressively harder
business questions on the `bi_course` star schema. **Every KPI is shown
three ways:**

```{tip}
A drop-in companion notebook with all four KPIs in both SQL and PySpark
form is at
[`notebooks/02_bi_with_sql.ipynb`](notebooks/02_bi_with_sql.ipynb).
```


1. **The SSIS data flow** you would have built in your previous BI lab —
   one bullet list per query.
2. **The lakehouse SQL** you run against the serverless warehouse.
3. **The PySpark DataFrame** equivalent — same logical plan, different API.
   AI4CI and HPC-Big-Data students should walk away comfortable with both.

```{contents}
:local:
:depth: 2
```

## Pre-flight

In your `bi_course` notebook, switch a Python cell to set the database once:

```python
spark.sql("USE workspace.bi_course")
```

And in any SQL cell:

```sql
USE workspace.bi_course;
```

All examples below assume those two `USE`s are in scope.

---

## KPI 1 — Revenue by product

> *"Which products bring us the most money?"*

### SSIS design

- **OLE DB Source** on `fact_sales`.
- **Lookup** on `dim_product` by `product_id` to pull `product_name`.
- **Aggregate** transformation: `SUM(revenue)` grouped by `product_name`.
- **Sort** transformation: descending by `total_revenue`.
- **OLE DB Destination** (or Data Reader) for the report.

### SQL on Databricks

```sql
SELECT
    p.product_name,
    SUM(f.revenue) AS total_revenue,
    SUM(f.quantity) AS units_sold
FROM fact_sales        f
JOIN dim_product       p USING (product_id)
GROUP BY p.product_name
ORDER BY total_revenue DESC;
```

### PySpark DataFrame

```python
from pyspark.sql import functions as F

fact    = spark.table("fact_sales")
product = spark.table("dim_product")

kpi1 = (
    fact.join(product, on="product_id", how="inner")
        .groupBy("product_name")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy(F.col("total_revenue").desc())
)
kpi1.show()
```

```{note}
**Side-by-side comment.** The SSIS pipeline has **5 boxes** dragged onto a
canvas. The SQL and PySpark versions are each **one statement**. The
warehouse plans the join, aggregate, and sort together and pushes them down
to the underlying Parquet files — you don't have to think about row buffers,
memory pressure or Lookup cache modes.
```

---

## KPI 2 — Daily revenue trend

> *"What does our daily revenue line look like, and where is the biggest
> single-day jump?"*

### SSIS design

- **OLE DB Source** on `fact_sales`.
- **Aggregate** by `order_date`.
- **Sort** by `order_date` ascending.
- A second pass with a **Script Component** (or a staged self-join) to
  compute *previous-day revenue* and the *day-over-day delta*.

### SQL on Databricks — using a window function

```sql
WITH daily AS (
    SELECT
        order_date,
        SUM(revenue) AS daily_revenue
    FROM fact_sales
    GROUP BY order_date
)
SELECT
    order_date,
    daily_revenue,
    LAG(daily_revenue) OVER (ORDER BY order_date) AS prev_day_revenue,
    daily_revenue
        - COALESCE(LAG(daily_revenue) OVER (ORDER BY order_date), 0)
        AS dod_delta
FROM daily
ORDER BY order_date;
```

### PySpark DataFrame

```python
from pyspark.sql import Window

w = Window.orderBy("order_date")

daily = (
    spark.table("fact_sales")
         .groupBy("order_date")
         .agg(F.sum("revenue").alias("daily_revenue"))
)

kpi2 = (
    daily.withColumn("prev_day_revenue", F.lag("daily_revenue").over(w))
         .withColumn(
             "dod_delta",
             F.col("daily_revenue") - F.coalesce(F.col("prev_day_revenue"), F.lit(0)),
         )
         .orderBy("order_date")
)
kpi2.show()
```

```{note}
**SSIS parallel — `LAG()` is a 30-line Script Component you no longer
write.** The window function does in one clause what an SSIS *Script
Component* (with a buffer-row-state variable) or a self-join with
`ROW_NUMBER()` would have taken 20+ lines to express.
```

---

## KPI 3 — Region × product matrix with subtotals

> *"Show revenue by region and product, plus regional and grand totals."*

### SSIS design

- **OLE DB Source** on `fact_sales`.
- Two **Lookups** (`dim_product`, `dim_region`).
- **Multicast** to two parallel **Aggregate** branches:
  *(region, product)* and *(region)*.
- **Union All** the three result sets (detail + region totals + grand total),
  carefully managing `NULL`s as a "subtotal marker".

### SQL on Databricks — `GROUP BY ROLLUP`

```sql
SELECT
    COALESCE(r.region_name, '— Grand Total —') AS region,
    COALESCE(p.product_name, '* All products *') AS product,
    SUM(f.revenue) AS revenue
FROM fact_sales        f
JOIN dim_region        r USING (region_id)
JOIN dim_product       p USING (product_id)
GROUP BY ROLLUP (r.region_name, p.product_name)
ORDER BY r.region_name NULLS LAST,
         p.product_name NULLS LAST;
```

`ROLLUP` produces:

- one row per *(region, product)* pair — the matrix cells,
- one row per region with `product = NULL` — the regional subtotals,
- one final row with both `NULL` — the grand total.

### PySpark DataFrame

```python
fact    = spark.table("fact_sales")
product = spark.table("dim_product")
region  = spark.table("dim_region")

kpi3 = (
    fact.join(region,  on="region_id",  how="inner")
        .join(product, on="product_id", how="inner")
        .rollup("region_name", "product_name")
        .agg(F.sum("revenue").alias("revenue"))
        .orderBy("region_name", "product_name")
)
kpi3.show(50)
```

```{note}
**SSIS parallel — what `ROLLUP` saves you.** In SSIS you would build the
*Multicast → two Aggregate branches → Union All* shape and manually emit a
`NULL` to mark the subtotal rows. `ROLLUP` does it natively and the result
set keeps a clean, well-typed schema.
```

---

## KPI 4 — Top-N per region (window function ranking)

> *"For each region, list the top 2 products by revenue."*

### SSIS design

This is the textbook example of *the thing SSIS makes painful*: you need a
*Sort* per region partition, a *Script Component* (or a staged
`ROW_NUMBER()` query in T-SQL) to rank, then a *Conditional Split* to keep
only `rank ≤ 2`. It is several hundred picas of canvas in SSIS.

### SQL on Databricks

```sql
WITH ranked AS (
    SELECT
        r.region_name,
        p.product_name,
        SUM(f.revenue) AS product_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY r.region_name
            ORDER BY     SUM(f.revenue) DESC
        ) AS rn
    FROM fact_sales       f
    JOIN dim_region       r USING (region_id)
    JOIN dim_product      p USING (product_id)
    GROUP BY r.region_name, p.product_name
)
SELECT region_name, product_name, product_revenue
FROM   ranked
WHERE  rn <= 2
ORDER BY region_name, product_revenue DESC;
```

### PySpark DataFrame

```python
from pyspark.sql import Window

agg = (
    spark.table("fact_sales")
         .join(spark.table("dim_region"),  on="region_id",  how="inner")
         .join(spark.table("dim_product"), on="product_id", how="inner")
         .groupBy("region_name", "product_name")
         .agg(F.sum("revenue").alias("product_revenue"))
)

w = Window.partitionBy("region_name").orderBy(F.col("product_revenue").desc())

kpi4 = (
    agg.withColumn("rn", F.row_number().over(w))
       .filter("rn <= 2")
       .orderBy("region_name", F.col("product_revenue").desc())
       .drop("rn")
)
kpi4.show()
```

---

## Stretch — promote KPIs to **gold** views for the dashboard

The Module 3 dashboard will not re-type these queries; it will read three
**views** that we promote to "gold" once and forget about. In the SSIS
world this is the equivalent of the *stored procedure* that SSRS reports
called.

```sql
USE workspace.bi_course;

CREATE OR REPLACE VIEW v_kpi_revenue_by_product AS
SELECT
    p.product_name,
    SUM(f.revenue)  AS total_revenue,
    SUM(f.quantity) AS units_sold
FROM fact_sales  f
JOIN dim_product p USING (product_id)
GROUP BY p.product_name;

CREATE OR REPLACE VIEW v_kpi_daily_revenue AS
SELECT order_date, SUM(revenue) AS daily_revenue
FROM   fact_sales
GROUP BY order_date;

CREATE OR REPLACE VIEW v_kpi_revenue_by_region_product AS
SELECT
    r.region_name,
    p.product_name,
    SUM(f.revenue) AS revenue
FROM fact_sales  f
JOIN dim_region  r USING (region_id)
JOIN dim_product p USING (product_id)
GROUP BY r.region_name, p.product_name;
```

```{seealso}
A view in Unity Catalog is *governed* — it inherits the catalog's permissions
and shows up in lineage. In SSRS-land, the closest analogue is "deploy the
stored procedure to a `Reporting` schema and lock it down with `GRANT
EXECUTE`".
```

## Free Edition check

- All four KPIs run in well under 5 s on a warm serverless warehouse.
- `ROLLUP`, window functions, and `MERGE INTO` are all available on the
  Free Edition warehouse — no paid SKU needed.
- The PySpark cells require *Serverless* notebook compute attached to your
  notebook; if PySpark fails with "no compute attached", re-attach the
  notebook to the *Serverless* runtime.

## Recap

You have now done in 40 minutes what an SSIS BI lab would have spent two
sessions on:

- 4 KPIs, each with the SSIS design **and** two lakehouse implementations.
- 3 gold views ready to feed Module 3's dashboard.
- A standing claim that you can read **and** write both Spark SQL and
  Spark DataFrame code.

On to [Module 3 — AI/BI Dashboard, Genie, governance, wrap-up](module-3-dashboard-and-wrapup.md).
