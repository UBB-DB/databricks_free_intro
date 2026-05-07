---
title: Module 3 — Dashboard, Genie, wrap-up
short_title: Module 3
---

# Module 3 (20 min) — AI/BI Dashboard, Genie, governance, wrap-up

In the last 20 minutes we publish the three gold views from Module 2 as an
**AI/BI Dashboard**, see what **AI/BI Genie** can do over the same schema,
take a quick **Unity Catalog** governance tour, and hand out a take-home
that lets students migrate one of their previous **SSIS** labs to Databricks.

```{tip}
Run [`notebooks/03_gold_views.ipynb`](notebooks/03_gold_views.ipynb) first
— it materialises the `v_kpi_*` views the dashboard binds to. Everything
else in this module happens in the Databricks UI.
```

```{contents}
:local:
:depth: 2
```

## 1. Build the AI/BI Dashboard (10 min)

> **SSIS parallel.** This is the **SSRS / Power BI** report you would have
> built from a stored procedure — but governed by Unity Catalog and
> refreshable from any browser, with no Reporting Services installation.

1. Left rail → **Dashboards → +Create dashboard** → name it
   `BI Course — Sales Overview`.
2. **Data → Add data → from Unity Catalog**, pick:
   - `workspace.bi_course.v_kpi_revenue_by_product`
   - `workspace.bi_course.v_kpi_daily_revenue`
   - `workspace.bi_course.v_kpi_revenue_by_region_product`
3. Drop three visualisations onto the canvas:

   | Tile | Source view | Type | Notes |
   | --- | --- | --- | --- |
   | Revenue by product | `v_kpi_revenue_by_product` | Bar (horizontal) | Sort `total_revenue` desc, top 10. |
   | Daily revenue trend | `v_kpi_daily_revenue` | Line | X = `order_date`, Y = `daily_revenue`. |
   | Region × product matrix | `v_kpi_revenue_by_region_product` | Heatmap | X = `region_name`, Y = `product_name`, value = `revenue`. |

4. Add a **filter widget** on `region_name` (multi-select) and bind it to
   the heatmap and to the bar chart.
5. Click **Publish** → choose *anyone with workspace access* (this is just
   you on Free Edition).

```{tip}
Each tile is just a SQL query against a serverless warehouse — open the
*"View query"* menu on a tile to see exactly what the dashboard is running.
That is precisely the same pattern you used in SSRS: *one stored proc per
report region*. The only difference is that here the "stored procs" are
**governed views** in Unity Catalog.
```

## 2. AI/BI Genie — natural language, no SSIS analogue (5 min)

> **No SSIS / SSRS analogue.** Genie is the lakehouse-native capability we
> highlighted in the intro: a chat interface over a Unity Catalog schema.

1. Left rail → **Genie → New space** → name it `BI Course — Genie`.
2. **Add tables**: pick all three tables under
   `workspace.bi_course` (`fact_sales`, `dim_product`, `dim_region`).
3. Optional but recommended — **add an instruction**:

   ```text
   Always join fact_sales to dim_product via product_id and to dim_region
   via region_id. Treat revenue as the primary measure. The grain of
   fact_sales is one order line per row.
   ```

4. Try these questions in the chat:

   - *"Which product has the highest total revenue?"*
   - *"Show monthly revenue by region as a line chart."*
   - *"Top 2 products per region by revenue."*

Genie translates each one into SQL against the same warehouse as the
dashboard, so your students can read **and verify** the generated query.

```{seealso}
Treat Genie as the **AI-augmented SSMS query window**: it suggests a SQL
statement, you inspect it, you accept or reject. Useful for self-service
analysts; it does not replace your ability to write SQL.
```

## 3. Unity Catalog governance walkthrough (3 min)

> **SSIS parallel.** This is what **SSISDB** tried to be — a single catalog
> for every BI asset — plus column-level **lineage** that SSIS never had.

In **Catalog Explorer**, navigate `workspace → bi_course` and click around:

- The **Tables** tab lists `fact_sales`, `dim_product`, `dim_region`.
- The **Sample Data** tab shows the first 100 rows — no `SELECT TOP 100`
  needed.
- The **Lineage** panel for `v_kpi_revenue_by_product` shows the upstream
  tables (`fact_sales`, `dim_product`) and any downstream consumers
  (your dashboard, your Genie space).
- The **Permissions** tab — try `SHOW GRANTS ON SCHEMA workspace.bi_course;`
  in a SQL cell.

```sql
SHOW GRANTS ON SCHEMA workspace.bi_course;
SHOW GRANTS ON TABLE  workspace.bi_course.fact_sales;
```

```{important}
Free Edition is single-account, so a live `GRANT SELECT ON TABLE … TO some_user`
demo has no second user to grant to. The interesting half — column-level
lineage and `SHOW GRANTS` — works fine, and we cover the multi-user side as
a reference in [`appendix-unity-catalog.md`](appendix-unity-catalog.md).
```

## 4. Mini-assignment (in-session, 2 min)

Answer in 3–4 bullets in your notebook (last cell):

- Which **single product** would you push next quarter, and why?
- Which **region × product** combination is currently underperforming
  relative to its region average?
- What **extra column** would reduce the uncertainty in your recommendation
  (cost? margin? customer segment? marketing spend?)?
- One question you would like to ask **Genie** that the current dataset
  cannot yet answer.

## 5. Take-home extensions (the SSIS migration path)

Pick **one** and submit a notebook URL by the next lab. All four are
explicitly designed to bridge from your previous SSIS coursework.

1. **Medallion refactor.** Re-build `bi_course` with explicit `bronze_*`,
   `silver_*`, and `gold_*` tables. Bronze = the raw `INSERT … VALUES` from
   setup; silver = cleaned + joined; gold = the three `v_kpi_*` views.
2. **Type-2 SCD with `MERGE`.** Extend `dim_product` with `effective_from`,
   `effective_to`, `is_current`, and write the `MERGE` that closes the old
   row and inserts a new one when `list_price` changes. See
   [`appendix-ssis-to-databricks.md#type-2-scd`](appendix-ssis-to-databricks.md#type-2-scd).
3. **Schedule a Lakeflow Job.** Wrap your KPI notebook in a Lakeflow Job,
   schedule it for every weekday at 07:00 UTC. *This replaces the SQL
   Server Agent job that would have invoked your SSIS package.*
4. **Migrate one of your previous SSIS labs.** Take any `.dtsx` package from
   your university BI lab, port the data flow to a Databricks notebook, and
   recreate the report as an AI/BI Dashboard. Use
   [`appendix-ssis-to-databricks.md`](appendix-ssis-to-databricks.md) as the
   step-by-step recipe.

## 6. Instructor close-out (2 min)

- Recap the **lakehouse value proposition**: one platform, one governance
  layer, ACID open files, native BI + AI on the same data.
- Highlight the **Free Edition** so students can keep practising at zero
  cost.
- Point at **next steps** on the official [Free Edition](https://www.databricks.com/learn/free-edition)
  resources: Databricks Academy, Generative AI Fundamentals, the community
  forum.

## Free Edition check

- Dashboard publishes successfully with the warehouse running — yes.
- Genie space accepts the three tables and answers at least one
  natural-language question — yes.
- `SHOW GRANTS` returns at least the workspace owner — yes (you).

## Course recap — what you can now do

You walked into a 2-hour session knowing **SQL Server + SSIS + SSMS** and
walked out able to:

- Run **ACID** operations on **Delta** tables and use **time travel**.
- Replace an **SSIS Slowly Changing Dimension** wizard with **`MERGE INTO`**.
- Express the same BI question in **SQL** *and* **PySpark DataFrame**.
- Publish a governed **AI/BI Dashboard**.
- Ask natural-language questions of your data with **AI/BI Genie**.

That is a complete BI delivery path on the lakehouse, on a free account, in
two hours.
