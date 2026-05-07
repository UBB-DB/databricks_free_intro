---
title: Appendix — Unity Catalog reference
short_title: Unity Catalog
---

# Appendix — Unity Catalog reference

This appendix is a **reference card** for Unity Catalog. We deliberately
kept it out of the main 2-hour session because Free Edition is a
single-account environment, and the interesting governance commands
(`GRANT … TO some_other_user`) have nobody to grant to.

If/when you move to a paid Databricks workspace at a real employer, this
page is the cheat-sheet you'll come back to.

```{contents}
:local:
:depth: 2
```

## 1. The three-level namespace

```{mermaid}
flowchart LR
    metastore[Metastore<br/>per region, per account] --> cat1[Catalog<br/>e.g. workspace, prod, sandbox]
    metastore --> cat2[Catalog]
    cat1 --> sch1[Schema<br/>e.g. bi_course]
    cat1 --> sch2[Schema]
    sch1 --> tbl1[Table / view / function / volume]
    sch1 --> tbl2[Table / view / function / volume]
```

- **Metastore**: one per region per Databricks account. Holds the global
  identity and permissions model.
- **Catalog**: top-level grouping (e.g. one per environment:
  `dev`, `staging`, `prod`). Free Edition gives you a default catalog
  called `workspace`.
- **Schema** (sometimes still written *database*): groups related objects
  inside a catalog (e.g. `bi_course`).
- **Securable**: tables, views, functions, volumes, models, dashboards.

Every fully qualified name is `<catalog>.<schema>.<object>`. There is no
schema-only fallback like SQL Server's `dbo.fact_sales`.

```{tip}
**SSIS parallel.** Closest equivalent: SSISDB had *Folder → Project →
Package*. Unity Catalog generalises it to *Catalog → Schema → Securable*
and applies it to **everything** — not just packages.
```

## 2. Useful read-only commands (work on Free Edition)

All of these run in a SQL cell against your serverless warehouse.

```sql
-- What catalogs / schemas / tables can I see?
SHOW CATALOGS;
SHOW SCHEMAS IN workspace;
SHOW TABLES  IN workspace.bi_course;

-- Inspect an object.
DESCRIBE EXTENDED workspace.bi_course.fact_sales;
DESCRIBE HISTORY  workspace.bi_course.fact_sales;
DESCRIBE DETAIL   workspace.bi_course.fact_sales;

-- Who has what permissions?
SHOW GRANTS ON CATALOG workspace;
SHOW GRANTS ON SCHEMA  workspace.bi_course;
SHOW GRANTS ON TABLE   workspace.bi_course.fact_sales;
```

## 3. The grant model (read-only on Free Edition)

The general shape, for reference:

```sql
-- Grants are made TO principals (users, groups, service principals).
GRANT USE CATALOG  ON CATALOG workspace                         TO `analysts`;
GRANT USE SCHEMA   ON SCHEMA  workspace.bi_course               TO `analysts`;
GRANT SELECT       ON TABLE   workspace.bi_course.fact_sales    TO `analysts`;

-- Object owners can do anything; transfer ownership with:
ALTER SCHEMA workspace.bi_course OWNER TO `data_platform_team`;
```

```{warning}
On Free Edition the only principal that exists is **you**. The `GRANT`
statements above will succeed only if a real group / user exists. Treat
this section as a paid-workspace reference, not a live exercise.
```

## 4. Lineage — the killer feature

Click any table or view in **Catalog Explorer** and open the **Lineage**
tab. You get:

- **Upstream tables** that fed into this object (joined / aggregated).
- **Downstream consumers**: dashboards, Genie spaces, ML feature tables,
  notebooks, jobs.
- **Column-level** lineage when the source query is set-based — invaluable
  for impact analysis when a dimension changes.

```{seealso}
**SSIS parallel.** Closest historical equivalent: SQL Server *Data
Lineage* in SSIS, which only worked inside a single package and required
manual setup. Unity Catalog gives it to you across **every** asset
automatically — no agent, no scanner.
```

## 5. Volumes (file-storage analogue)

Tables hold structured data; **Volumes** hold files (CSV, JSON, PDF, …) and
are addressable as `/Volumes/<catalog>/<schema>/<volume>/<path>`. This is
the lakehouse equivalent of an SMB share or an SSIS *Flat File Connection
Manager*'s root folder, but governed by Unity Catalog.

```sql
-- Create a managed volume to land raw files into.
CREATE VOLUME IF NOT EXISTS workspace.bi_course.raw_drops;
```

You can then `LIST` and `COPY INTO`:

```sql
LIST '/Volumes/workspace/bi_course/raw_drops';

COPY INTO workspace.bi_course.fact_sales
FROM     '/Volumes/workspace/bi_course/raw_drops/sales_2026_q1.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
```

## 6. Cheat-sheet — what you should remember

- Every object name is **three parts**.
- `SHOW GRANTS ON <object>` answers *"who can read this?"*.
- `DESCRIBE HISTORY <delta_table>` answers *"who changed this when?"*.
- The **Lineage** tab in Catalog Explorer answers *"if I change this column,
  what breaks?"*.
- **Volumes** are the lakehouse-native way to store and govern files;
  `COPY INTO` is the lakehouse-native way to ingest from them — much closer
  to SSIS *Bulk Insert* than to a hand-written loop.
