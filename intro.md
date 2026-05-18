---
title: BI on the Lakehouse
subtitle: A 2-hour crash course for UBB Cluj CS students
---

# BI on the Lakehouse — 2-hour crash course

This is a ready-to-teach **Business Intelligence** crash course for students of the
[Faculty of Mathematics and Computer Science, Babeș-Bolyai University](https://www.cs.ubbcluj.ro/en/),
delivered entirely on [**Databricks Free Edition**](https://www.databricks.com/learn/free-edition)
so every exercise is reproducible at zero cost on a personal account.

The structure follows the spirit of the [Jupyter Book workshop template](https://github.com/jupyter-book/workshop-template/),
but content is fully rewritten for an audience that is comfortable with Python and SQL.

## Who this is for

CS students from any of UBB's tracks where data engineering / BI shows up.

<!-- - **BSc**: Computer Science, Information Engineering, Artificial Intelligence,
  Mathematics-Computer Science.
- **MSc**: *Data Science for Industry and Society*, *Artificial Intelligence for
  Connected Industries (AI4CI)*, *High Performance Computing and Big Data
  Analytics*, *Databases*, *Software Engineering*. -->

We assume you have already met **SQL Server, SSMS, SSIS, and SSRS / Power BI**
in your Business Intelligence labs. This course is therefore framed as
*"the lakehouse equivalent of the SSIS pipeline you already know"*, not as
a brand-new abstraction starting from zero.

## Learning outcomes

By the end of the core 2 hours (plus an optional extension module) you will be able to:

1. Explain what a **Data Lakehouse** is and where BI fits in it, contrasting it
   with the SQL-Server-and-SSIS stack you already know.
2. Use **Databricks Free Edition** to run notebooks against a **serverless SQL
   warehouse** governed by **Unity Catalog**.
3. Read and write **Delta Lake** tables; demonstrate ACID, **time travel**, and
   **`MERGE INTO`** as the lakehouse equivalent of SSIS *Slowly Changing Dimension*.
4. Translate a multi-step BI question into both **SQL** and **PySpark
   DataFrame** code, with the same SSIS data-flow design in mind for both.
5. Publish an **AI/BI Dashboard** and ask natural-language questions of your
   data with **AI/BI Genie**.
6. Build a **medallion pipeline** (bronze-silver-gold) and understand how to
  orchestrate it with a **Lakeflow Job**.

## SSIS / SSMS ↔ Databricks Rosetta stone

Every concept introduced later maps to something you already know. Keep this
mapping open in another tab during the workshop:

- **SSMS Object Explorer** ↔ Databricks **Catalog Explorer** (Unity Catalog,
  three-part names like `workspace.bi_course.fact_sales`).
- **SSMS query window** ↔ Databricks **SQL Editor** + a serverless SQL
  warehouse.
- **SSISDB / Integration Services Catalog** ↔ **Unity Catalog** — the governed
  namespace for every table, view, function and job.
- **SSIS `.dtsx` package** ↔ Databricks **notebook** (interactive) or
  **Lakeflow Job** (scheduled).
- **SSIS Control Flow** ↔ a **Lakeflow Job** with task dependencies (a DAG of
  notebook / SQL tasks).
- **SSIS Data Flow** ↔ a single PySpark / SQL transformation cell, or a
  **Lakeflow Spark Declarative Pipeline** (formerly Delta Live Tables) for a
  declarative ETL graph.
- **OLE DB Source** ↔ `SELECT … FROM <delta_table>` or
  `spark.read.table(...) / spark.read.format("csv")`.
- **OLE DB Destination** ↔ `INSERT INTO … SELECT …` or
  `df.write.saveAsTable(name, mode="append")`.
- **Lookup transformation** ↔ a SQL `JOIN` (or `df.join(...)` in PySpark) — and
  on the lakehouse it always runs at scale, you do not need to cache the lookup
  table in memory.
- **Derived Column / Conditional Split** ↔ SQL `CASE WHEN` /
  `df.withColumn("...", when(...).otherwise(...))`.
- **Aggregate transformation** ↔ `GROUP BY` / `df.groupBy(...).agg(...)`.
- **Sort transformation** ↔ `ORDER BY` (you usually fold it into a window
  function instead of a global sort, to avoid a full shuffle).
- **Slowly Changing Dimension wizard** ↔ Delta `MERGE INTO` (Type 1 =
  `WHEN MATCHED THEN UPDATE`; Type 2 = `WHEN NOT MATCHED THEN INSERT` plus a
  `current_flag` column).
- **SQL Server Agent schedule** ↔ a **Lakeflow Job** schedule (cron, on
  serverless compute).
- **SSIS package configurations / parameters** ↔ Notebook **widgets** + Job
  parameters.
- **SSIS error output / row redirect** ↔ Lakeflow Pipeline **expectations**
  with a quarantine table.
- **SQL Server transaction log** ↔ Delta Lake **transaction log**
  (`_delta_log/`) — this is what enables `DESCRIBE HISTORY` and
  `SELECT … VERSION AS OF`.
- **SSRS report / Power BI Desktop pinned to SharePoint** ↔ an
  **AI/BI Dashboard** published from the workspace.
- **No SSIS analogue** ↔ **AI/BI Genie** (natural-language Q&A over a Unity
  Catalog schema). This is a brand-new capability the lakehouse unlocks.

The full mapping with code-level examples lives in
[`appendix-ssis-to-databricks.md`](appendix-ssis-to-databricks.md).

For a role-based overview of the Databricks architecture (data engineer,
analyst, ML engineer), see
[`databricks-roles-and-lakehouse-overview.md`](databricks-roles-and-lakehouse-overview.md).

## Session plan (120 min core )

| Time | Block |
| --- | --- |
| 0:00 – 0:10 | Introduction and SSIS↔Databricks mapping ([`intro.md`](intro.md)) |
| 0:10 – 0:25 | Free Edition setup and workspace tour ([`setup-databricks-free.md`](setup-databricks-free.md)) |
| 0:25 – 0:55 | Module 1 — Lakehouse basics + Delta + time travel ([`module-1-lakehouse-basics.md`](module-1-lakehouse-basics.md)) |
| 0:55 – 1:35 | Module 2 — BI analysis with SQL + PySpark ([`module-2-bi-with-sql.md`](module-2-bi-with-sql.md)) |
| 1:35 – 1:55 | Module 3 — AI/BI Dashboard + Genie + governance ([`module-3-dashboard-and-wrapup.md`](module-3-dashboard-and-wrapup.md)) |
| 1:55 – 2:00 | Recap, take-home assignment, Q&A |
| 2:00 – 2:25 | Optional Module 4 — pipelines + medallion orchestration ([`module-4-pipelines-and-medallion.md`](module-4-pipelines-and-medallion.md)) |

## Prerequisites

- A laptop with a modern browser. **Nothing is installed locally** for the
  live session — every exercise runs in your browser against Databricks Free
  Edition.
- A free Databricks account (signup link in
  [`setup-databricks-free.md`](setup-databricks-free.md)).
- Optional, for the offline study path: Python 3.11+ and
  [`uv`](https://docs.astral.sh/uv/) — see
  [`appendix-pyspark-local.md`](appendix-pyspark-local.md).

## Drop-in notebooks

If you do not want to copy-paste cells from this book during the live
session, the [`notebooks/`](notebooks/) folder ships five ready-to-import
`.ipynb` files — one per module — that you can upload directly to your
Databricks Free Edition workspace. See
[`notebooks/README.md`](notebooks/README.md) for the three import paths
(raw GitHub URL, file drag-and-drop, or Git folder).

## Instructor notes

- Free Edition is single-account: do *not* design exercises that require two
  users sharing data. Treat any "share with a colleague" demo as a screenshot,
  not a live click-through.
- The serverless SQL warehouse has a **cold-start of ~30–60 s** on Free
  Edition. Start it at the beginning of the session and leave it running.
- All Unity Catalog object names are three-part:
  `workspace.bi_course.<table>`. Use them consistently from the first slide.
- The `myst build --html` build is offline; no network call goes to Databricks
  during the GitHub Pages build, so the site keeps building even if the
  Databricks endpoints change.
