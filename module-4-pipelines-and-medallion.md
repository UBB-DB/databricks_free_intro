---
title: Module 4 — Pipelines, medallion architecture, and orchestration
short_title: Module 4
---

# Module 4 (25 min) — Pipelines, medallion ETL, and Lakeflow orchestration

By the end of this module you will understand the **medallion (bronze-silver-gold)
architecture**, implement reusable transformation pipelines using **Databricks
Workflows**, and orchestrate them into a reliable ETL schedule — the modern
lakehouse equivalent of **SQL Server Agent scheduling your SSIS packages**.

```{tip}
The companion transformation files are in [`pipelines/transformations/`](pipelines/transformations/)
organized by layer. A hands-on notebook is at
[`notebooks/04_pipelines.ipynb`](notebooks/04_pipelines.ipynb).
```

```{contents}
:local:
:depth: 2
```

## 1. The medallion architecture (5 min)

> **SSIS parallel.** Bronze = raw data extracted from source systems (your
> SSIS `OLE DB Source`). Silver = cleaned, conformed, and deduplicated. Gold =
> aggregated, business-ready dimensions and facts for reporting.

```{mermaid}
graph LR
    subgraph bronze ["🥉 Bronze<br/>(Raw)"]
        b1["bronze_hr_dataset<br/>Raw HR data<br/>+ light schema fixes"]
    end
    subgraph silver ["🥈 Silver<br/>(Conformed)"]
        s1["dim_employee<br/>Employee attributes"]
        s2["dim_department<br/>Departments + managers"]
        s3["dim_location<br/>Location details"]
        s4["dim_date<br/>Date dimension"]
        s5["fact_employee<br/>Employee facts<br/>+ foreign keys"]
    end
    subgraph gold ["🥇 Gold<br/>(Business Ready)"]
        g1["gold_company_summary<br/>Org-wide KPIs"]
        g2["gold_department_metrics<br/>Dept-level rollups"]
        g3["gold_hiring_trends<br/>Temporal hiring patterns"]
        g4["gold_location_analytics<br/>Location-based insights"]
    end
    src["HR Dataset<br/>Source System"] --> b1
    b1 --> s1 & s2 & s3 & s4 & s5
    s1 & s2 & s3 & s4 & s5 --> g1 & g2 & g3 & g4
    g1 & g2 & g3 & g4 --> dashboard["📊 Dashboards<br/>AI/BI Genie<br/>Reports"]
```

Each layer serves a purpose:

| Layer | Purpose | Freshness | Retention | Example |
| --- | --- | --- | --- | --- |
| **Bronze** | Ingest raw data, light schema fixes | As loaded | All history | `bronze_hr_dataset` |
| **Silver** | Conform, deduplicate, join to keys | Incremental | 1–2 years | `dim_employee`, `fact_employee` |
| **Gold** | Aggregate, materialize KPIs | Daily/hourly | Time-bound | `gold_department_metrics` |

```{note}
**Why medallion?** Separates concerns:
- ETL engineers own **bronze & silver** (data quality).
- BI analysts consume **gold** (known-good KPIs).
- If a BI request changes, you fix gold; bronze/silver stay stable.
- You can replay any layer without reingesting from the source.
```

## 2. Bronze layer — raw ingestion

The bronze layer does **minimal transformation**: read the source, fix obvious
schema issues, and land it as a Delta table.

```python
# pipelines/transformations/bronze/bronze_hr_dataset.py
from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer - raw HR dataset ingestion from workspace.default.hr_dataset"
)
def bronze_hr_dataset():
    return spark.read.table("workspace.default.hr_dataset") \
        .withColumnRenamed("EDUCATION LEVEL", "EDUCATION_LEVEL")
```

**What it does:**
1. Reads the raw `hr_dataset` table from the default warehouse.
2. Renames `EDUCATION LEVEL` → `EDUCATION_LEVEL` (spaces break SQL).
3. Returns a materialized view — Databricks will manage the Delta file.

**SSIS parallel:** This is your first `OLE DB Source` + `Derived Column`
(for the rename) before you drop into your data warehouse.

## 3. Silver layer — conformed dimensions and facts

Silver applies business logic: filter nulls, cast types, create foreign keys.

### 3a. Dimensions

A **dimension** is a lookup table (immutable attributes per key). Our example:
**employees, departments, locations, and dates**.

```python
# pipelines/transformations/silver/dim_employee.py
from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Employee dimension - employee attributes",
    cluster_by=["EMP_ID"]
)
def dim_employee():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            "EMP_ID",
            "EMP_NM",
            "EMAIL",
            "EDUCATION_LEVEL" 
        )
        .distinct()
    )
```

**What it does:**
- Selects employee attributes (id, name, email, education).
- `.distinct()` removes duplicates.
- `cluster_by=["EMP_ID"]` tells Delta to physically sort by `EMP_ID` — speeds
  up joins on that key.

```python
# pipelines/transformations/silver/dim_department.py
@dp.materialized_view(
    comment="Department dimension - department details with managers",
    cluster_by=["DEPARTMENT"]
)
def dim_department():
    return (
        spark.read.table("bronze_hr_dataset")
        .select("DEPARTMENT", "MANAGER")
        .distinct()
    )
```

```python
# pipelines/transformations/silver/dim_date.py
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Date dimension - derived from hire dates, start dates, and end dates",
    cluster_by=["date"]
)
def dim_date():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(F.col("HIRE_DT").alias("date_col"))
        .union(
            spark.read.table("bronze_hr_dataset")
            .select(F.col("START_DT").alias("date_col"))
        )
        .union(
            spark.read.table("bronze_hr_dataset")
            .select(F.col("END_DT").alias("date_col"))
        )
        .filter(F.col("date_col").isNotNull())
        .select(
            F.col("date_col").cast("date").alias("date"),
            F.year("date_col").alias("year"),
            F.month("date_col").alias("month"),
            F.quarter("date_col").alias("quarter"),
            F.dayofmonth("date_col").alias("day"),
            F.dayofweek("date_col").alias("day_of_week"),
            F.weekofyear("date_col").alias("week_of_year")
        )
        .distinct()
    )
```

**What it does:**
- Unions all date columns from the source (`HIRE_DT`, `START_DT`, `END_DT`).
- Extracts year, month, quarter, day-of-week for time-based grouping.
- `.distinct()` ensures one row per unique date.

### 3b. Facts

A **fact table** is the transactional core: one row per event (hire, transfer,
etc.) plus foreign keys to dimensions.

```python
# pipelines/transformations/silver/fact_employee.py
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Employee fact table - contains employee measures and foreign keys to dimensions",
    cluster_by=["EMP_ID", "DEPARTMENT"]
)
def fact_employee():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            "EMP_ID",
            "JOB_TITLE",
            "DEPARTMENT",
            "LOCATION",
            F.col("HIRE_DT").cast("date").alias("HIRE_DT"),
            F.col("START_DT").cast("date").alias("START_DT"),
            F.col("END_DT").cast("date").alias("END_DT"),
            "SALARY"
        )
    )
```

**What it does:**
- Casts date columns to `date` type (removes time component).
- Selects foreign key columns: `EMP_ID`, `DEPARTMENT`, `LOCATION`.
- Selects measures: `SALARY`, and timestamps: `HIRE_DT`, `START_DT`, `END_DT`.
- `cluster_by=["EMP_ID", "DEPARTMENT"]` optimizes joins on employee and department.

**SSIS parallel:** This is your `Lookup` transforms + `OLE DB Destination`
writing to your warehouse fact table.

## 4. Gold layer — KPIs and business aggregates

Gold materializes the **final KPIs** that reports and dashboards consume. No
further joins needed; all metrics pre-computed.

### 4a. Company-wide summary

```python
# pipelines/transformations/gold/gold_company_summary.py
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold - Company-wide summary metrics (single row)"
)
def gold_company_summary():
    return (
        spark.read.table("fact_employee")
        .agg(
            F.lit("Company Wide").alias("METRIC_LEVEL"),
            F.current_timestamp().alias("SNAPSHOT_TIMESTAMP"),
            F.count("EMP_ID").alias("TOTAL_EMPLOYEES"),
            F.countDistinct("DEPARTMENT").alias("TOTAL_DEPARTMENTS"),
            F.countDistinct("LOCATION").alias("TOTAL_LOCATIONS"),
            F.countDistinct("JOB_TITLE").alias("TOTAL_JOB_TITLES"),
            F.sum("SALARY").alias("TOTAL_PAYROLL"),
            F.avg("SALARY").alias("AVG_SALARY"),
            F.percentile_approx("SALARY", 0.5).alias("MEDIAN_SALARY"),
            F.min("SALARY").alias("MIN_SALARY"),
            F.max("SALARY").alias("MAX_SALARY"),
            F.min("HIRE_DT").alias("EARLIEST_HIRE_DATE"),
            F.max("HIRE_DT").alias("LATEST_HIRE_DATE")
        )
        .withColumn(
            "AVG_SALARY_PER_DEPARTMENT",
            F.col("TOTAL_PAYROLL") / F.col("TOTAL_DEPARTMENTS")
        )
        .withColumn(
            "SALARY_RANGE",
            F.col("MAX_SALARY") - F.col("MIN_SALARY")
        )
    )
```

**What it does:**
- Aggregates all employees into **one row** of company-wide metrics.
- Counts distinct departments, locations, job titles.
- Computes salary statistics (min, max, avg, median, total, range).
- Adds a `SNAPSHOT_TIMESTAMP` to track when the metrics were computed.

### 4b. Department-level rollups

```python
# pipelines/transformations/gold/gold_department_metrics.py
@dp.materialized_view(
    comment="Gold - Department-level aggregated metrics for business reporting",
    cluster_by=["DEPARTMENT"]
)
def gold_department_metrics():
    return (
        spark.read.table("fact_employee")
        .groupBy("DEPARTMENT")
        .agg(
            F.count("EMP_ID").alias("EMPLOYEE_COUNT"),
            F.sum("SALARY").alias("TOTAL_SALARY"),
            F.avg("SALARY").alias("AVG_SALARY"),
            F.min("SALARY").alias("MIN_SALARY"),
            F.max("SALARY").alias("MAX_SALARY"),
            F.percentile_approx("SALARY", 0.5).alias("MEDIAN_SALARY"),
            F.countDistinct("JOB_TITLE").alias("UNIQUE_JOB_TITLES"),
            F.countDistinct("LOCATION").alias("UNIQUE_LOCATIONS"),
            F.min("HIRE_DT").alias("EARLIEST_HIRE_DATE"),
            F.max("HIRE_DT").alias("LATEST_HIRE_DATE")
        )
    )
```

**What it does:**
- `groupBy("DEPARTMENT")` creates one row per department.
- Pre-computes all salary statistics per department.
- Useful for org charts, department budgets, hiring dashboards.

### 4c. Temporal trends

```python
# pipelines/transformations/gold/gold_hiring_trends.py
@dp.materialized_view(
    comment="Gold - Monthly hiring trends with cumulative metrics",
    cluster_by=["HIRE_YEAR", "HIRE_MONTH"]
)
def gold_hiring_trends():
    return (
        spark.read.table("fact_employee")
        .withColumn("HIRE_YEAR", F.year("HIRE_DT"))
        .withColumn("HIRE_MONTH", F.month("HIRE_DT"))
        .withColumn("HIRE_QUARTER", F.quarter("HIRE_DT"))
        .groupBy("HIRE_YEAR", "HIRE_QUARTER", "HIRE_MONTH", "DEPARTMENT")
        .agg(
            F.count("EMP_ID").alias("HIRES_COUNT"),
            F.avg("SALARY").alias("AVG_STARTING_SALARY"),
            F.countDistinct("JOB_TITLE").alias("UNIQUE_ROLES_HIRED")
        )
        .withColumn(
            "YEAR_MONTH",
            F.concat(
                F.col("HIRE_YEAR"),
                F.lit("-"),
                F.lpad(F.col("HIRE_MONTH"), 2, "0")
            )
        )
    )
```

**What it does:**
- Extracts year, month, quarter from `HIRE_DT`.
- `groupBy()` by all four time dimensions + department.
- Pre-computes hiring volume and average starting salary per month per dept.
- Constructs a `YEAR_MONTH` string for easy sorting in dashboards.

### 4d. Location analytics

```python
# pipelines/transformations/gold/gold_location_analytics.py
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Gold - Location-based workforce and compensation analytics",
    cluster_by=["LOCATION"]
)
def gold_location_analytics():
    return (
        spark.read.table("fact_employee")
        .groupBy("LOCATION")
        .agg(
            F.count("EMP_ID").alias("EMPLOYEE_COUNT"),
            F.countDistinct("DEPARTMENT").alias("DEPARTMENTS_COUNT"),
            F.countDistinct("JOB_TITLE").alias("JOB_TITLES_COUNT"),
            F.avg("SALARY").alias("AVG_SALARY"),
            F.sum("SALARY").alias("TOTAL_SALARY"),
            F.percentile_approx("SALARY", 0.5).alias("MEDIAN_SALARY"),
            F.stddev("SALARY").alias("SALARY_STDDEV")
        )
        .withColumn(
            "SALARY_COST_RANK",
            F.dense_rank().over(
                Window.orderBy(F.desc("TOTAL_SALARY"))
            )
        )
        .withColumn(
            "HEADCOUNT_RANK",
            F.dense_rank().over(
                Window.orderBy(F.desc("EMPLOYEE_COUNT"))
            )
        )
    )
```

**What it does:**
- Groups by location to see workforce across offices.
- Computes salary stats and standard deviation.
- Uses **window functions** (`dense_rank`) to rank locations by cost and headcount
  — useful for "top 3 most expensive offices" queries.

## 5. Orchestration with Lakeflow Jobs (8 min)

> **SSIS parallel.** This is your **SQL Server Agent job** that runs your SSIS
> package on a schedule — but serverless, with full lineage and error handling.

A **Lakeflow Job** is a serverless workflow that:
1. Runs PySpark or SQL code on a schedule.
2. Tracks lineage (which tables upstream, which downstream).
3. Retries on failure; sends alerts.
4. No infrastructure to manage.

### Create a Lakeflow Job

1. In Databricks, **Workflows → New Job** → name it `hr_medallion_pipeline`.
2. **Task 1: Bronze ingestion**
   - Click **Tasks → + Add task**.
   - Name: `ingest_bronze`.
   - Type: **Notebook**.
   - Notebook: *select your notebook with the bronze transformation code* (or
     upload a script from `pipelines/transformations/bronze/bronze_hr_dataset.py`).
   - Cluster: **Serverless SQL** (automatic, cost-effective).
   - Click **Create task**.

3. **Task 2: Silver conforming** (depends on Task 1)
   - Click **+ Add task → Notebook**.
   - Name: `conform_silver`.
   - Notebook: select script from `pipelines/transformations/silver/`.
   - **Set upstream dependency**: Click the task, under **Depends on**, add
     `ingest_bronze`.
   - Click **Create task**.

   ```{tip}
   In Lakeflow, tasks run in **topological order**: if `conform_silver`
   depends on `ingest_bronze`, the system runs bronze first.
   ```

4. **Task 3: Gold aggregation** (depends on Task 2)
   - Click **+ Add task → Notebook**.
   - Name: `materialize_gold`.
   - Notebook: select one or all scripts from `pipelines/transformations/gold/`.
   - Depends on: `conform_silver`.
   - Click **Create task**.

5. **Schedule the job**
   - Click the **Job name** to return to the job editor.
   - Under **Schedule**, click **Trigger → Add trigger**.
   - Type: **Scheduled**.
   - Frequency: **Every day**, time: **07:00 UTC** (adjust to your timezone).
   - Click **Save job**.

6. **Run it now to validate**
   - Click **Run now** (top right). Wait ~2 min for all tasks to complete.
   - Once all tasks show ✓, the pipeline is working.

### Monitor and debug

- **Job runs page**: Click **Job name → All runs** to see a history of past
  executions.
- **Run details**: Click a run to see task start/stop times, logs, and errors.
- **Lineage**: In **Catalog Explorer**, view upstream/downstream table lineage
  — Databricks auto-detects that `gold_hiring_trends` depends on
  `fact_employee` depends on `bronze_hr_dataset`.

```{seealso}
For multi-cluster pipelines (e.g., separate clusters for bronze and gold),
see [`appendix-ssis-to-databricks.md#lakeflow-advanced`](appendix-ssis-to-databricks.md#lakeflow-advanced).
```

## 6. Mini-assignment (in-session, 5 min)

Answer in a notebook cell:

1. **What data quality check** would you add to the bronze layer? (E.g., reject
   rows where `EMP_ID` is null, or salary < 0.)
2. **Which gold table** do you think a CFO would query first, and why?
3. **If the source HR dataset changed schema** (e.g., a new column `HIRE_REASON`
   added), which layer(s) would you need to update? Why?
4. **Write a 4-line PySpark query** that reads `gold_department_metrics` and
   finds the department with the highest median salary.

## 7. Free Edition check

- [ ] Bronze, silver, and gold transformation scripts run without error.
- [ ] At least one gold table is populated (run `SELECT * FROM gold_company_summary LIMIT 1`).
- [ ] Lakeflow Job is created and runs successfully on manual trigger.
- [ ] `SHOW TABLES` in your schema lists all bronze, silver, and gold tables.

## Course recap — what you can now do

You started with **raw data** and built:

- A **medallion pipeline** that separates concerns (ingest → conform → aggregate).
- **Modular transformations** using PySpark that you can version-control and
  code-review (instead of SSIS GUI boxes).
- A **serverless orchestration** job that replaces SQL Server Agent.
- **Column-level lineage** in Unity Catalog so you know which BI assets
  depend on which tables.

You can now **build production BI systems** on the lakehouse: ingest raw data,
conform it, materialize KPIs, and serve them to dashboards and AI/BI Genie —
all governed and lineage-aware, on a free account.
