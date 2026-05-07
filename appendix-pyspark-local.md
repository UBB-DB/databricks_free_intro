---
title: Appendix — Local PySpark via uv
short_title: Local PySpark
---

# Appendix — Local PySpark via uv

This appendix is the **offline-study fallback**. If you lose access to your
Databricks Free Edition account, or if you want to step through the Module 2
KPIs on a long flight, you can reproduce the entire dataset and every query
on your laptop with **vanilla open-source PySpark**, managed by `uv`.

```{important}
This is *not* a Databricks Connect tutorial. Free Edition does not support
Databricks Connect from a local IDE, so we deliberately use plain
open-source PySpark on a CSV instead. The SQL and DataFrame code is
identical to what you ran in the workshop — only the catalog layer differs.
```

```{contents}
:local:
:depth: 2
```

## 1. Install `uv`

`uv` is an extremely fast Python package and project manager from Astral.
Install it once:

- **macOS / Linux**: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Windows (PowerShell)**:
  `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
- Or via your system package manager: `brew install uv`, `pipx install uv`.

Verify:

```bash
uv --version
```

## 2. Sync the project

From the repo root:

```bash
uv sync
```

This will:

- Create a `.venv/` in the repo (Python 3.11, pinned by `.python-version`).
- Install everything from `pyproject.toml` (PySpark, pandas, JupyterLab,
  the Jupyter Book v2 launcher).

Activate the env if you want a normal shell:

```bash
# macOS / Linux
source .venv/bin/activate
# Windows (PowerShell)
.venv\Scripts\Activate.ps1
```

Or just use `uv run …` to run a one-off command in the env without
activation.

```{warning}
PySpark needs a JVM. You must have **Java 17 (or 11)** installed and on
`PATH`. On macOS: `brew install openjdk@17`. On Windows: install
[Adoptium Temurin 17](https://adoptium.net/) and make sure `JAVA_HOME`
points at it.
```

## 3. Write the sample CSV (same data as the workshop)

Save this as `data/bi_course.py` (the `data/` folder is git-ignored on
purpose — re-create it locally):

```python
import pandas as pd
from pathlib import Path

OUT = Path(__file__).parent

dim_product = pd.DataFrame(
    [
        (1, 'Laptop 14"',     'Computers',   1200.00),
        (2, 'Laptop 16"',     'Computers',   1800.00),
        (3, 'Wireless Mouse', 'Accessories',  25.00),
        (4, 'Mech. Keyboard', 'Accessories',  80.00),
        (5, 'Monitor 27"',    'Displays',    300.00),
        (6, 'Monitor 32"',    'Displays',    520.00),
    ],
    columns=["product_id", "product_name", "category", "list_price"],
)

dim_region = pd.DataFrame(
    [
        (1, "North", "Romania"),
        (2, "South", "Romania"),
        (3, "East",  "Romania"),
        (4, "West",  "Romania"),
    ],
    columns=["region_id", "region_name", "country"],
)

fact_sales = pd.DataFrame(
    [
        (1001, "2026-01-15", 1, 1, 1, 1200.00),
        (1002, "2026-01-15", 3, 1, 2,   50.00),
        (1003, "2026-01-16", 1, 2, 1, 1100.00),
        (1004, "2026-01-16", 4, 2, 1,   80.00),
        (1005, "2026-01-17", 5, 4, 1,  300.00),
        (1006, "2026-01-17", 3, 4, 1,   25.00),
        (1007, "2026-01-18", 2, 3, 1, 1800.00),
        (1008, "2026-01-18", 6, 3, 1,  520.00),
        (1009, "2026-01-19", 1, 1, 2, 2400.00),
        (1010, "2026-01-19", 5, 2, 1,  300.00),
        (1011, "2026-01-20", 4, 4, 3,  240.00),
        (1012, "2026-01-20", 2, 3, 1, 1750.00),
    ],
    columns=["order_id", "order_date", "product_id", "region_id", "quantity", "revenue"],
)

dim_product.to_csv(OUT / "dim_product.csv", index=False)
dim_region.to_csv(OUT / "dim_region.csv",   index=False)
fact_sales.to_csv(OUT / "fact_sales.csv",   index=False)
print("CSV files written to", OUT)
```

Run it once:

```bash
mkdir -p data
uv run python data/bi_course.py
```

## 4. Run the KPIs locally

Save this as `kpi_demo.py` and run with `uv run python kpi_demo.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
        .appName("bi_course_local")
        .master("local[*]")
        .getOrCreate()
)

read_csv = lambda name: (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"data/{name}.csv")
)

fact    = read_csv("fact_sales")
product = read_csv("dim_product")
region  = read_csv("dim_region")

print("--- KPI 1: revenue by product ---")
(
    fact.join(product, on="product_id", how="inner")
        .groupBy("product_name")
        .agg(F.sum("revenue").alias("total_revenue"))
        .orderBy(F.col("total_revenue").desc())
        .show()
)

print("--- KPI 2: daily revenue + day-over-day delta ---")
w = Window.orderBy("order_date")
daily = fact.groupBy("order_date").agg(F.sum("revenue").alias("daily_revenue"))
(
    daily.withColumn("prev_day_revenue", F.lag("daily_revenue").over(w))
         .withColumn(
             "dod_delta",
             F.col("daily_revenue") - F.coalesce(F.col("prev_day_revenue"), F.lit(0)),
         )
         .orderBy("order_date")
         .show()
)

print("--- KPI 4: top 2 products per region ---")
agg = (
    fact.join(region,  on="region_id",  how="inner")
        .join(product, on="product_id", how="inner")
        .groupBy("region_name", "product_name")
        .agg(F.sum("revenue").alias("product_revenue"))
)
w_top = Window.partitionBy("region_name").orderBy(F.col("product_revenue").desc())
(
    agg.withColumn("rn", F.row_number().over(w_top))
       .filter("rn <= 2")
       .orderBy("region_name", F.col("product_revenue").desc())
       .drop("rn")
       .show()
)

spark.stop()
```

The output should match what you saw on the serverless warehouse, modulo
the `DECIMAL` precision printout (CSV inference picks `double`).

## 5. What you give up locally

- **No Delta** — local PySpark reads CSVs, not Delta tables (you can install
  `delta-spark` to get Delta locally, but it is out of scope here).
- **No Unity Catalog** — local SparkSession has no governance layer.
- **No AI/BI Dashboards / Genie** — those are platform features, not
  language features.

The point of this appendix is to keep the **code** practising muscle warm
when the **platform** is unavailable. As soon as you are back online, the
notebooks you wrote here drop into Databricks Free Edition unchanged
(replace `read.csv("data/...")` with `spark.table("workspace.bi_course....")`).
