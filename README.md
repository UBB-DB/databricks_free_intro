# databricks_free_intro

A 2-hour **Business Intelligence on the Lakehouse** crash course for CS students of the
[Faculty of Mathematics and Computer Science, Babeș-Bolyai University](https://www.cs.ubbcluj.ro/en/),
delivered entirely on [**Databricks Free Edition**](https://www.databricks.com/learn/free-edition).

The course is published as a [Jupyter Book v2 / MyST-MD](https://mystmd.org/) site
and deployed to GitHub Pages on every push to `main`.

> **Bridges from SQL Server / SSIS / SSMS** — every Databricks concept is introduced
> as the lakehouse equivalent of the SSIS pipeline, SSMS query window, or SSRS
> report you already met in your university BI lab.

## Contents

- [`intro.md`](intro.md) — course overview, learning outcomes, the **SSIS↔Databricks Rosetta stone**.
- [`setup-databricks-free.md`](setup-databricks-free.md) — Free Edition signup, workspace tour, serverless SQL warehouse, the 3-table star-schema sample dataset.
- [`module-1-lakehouse-basics.md`](module-1-lakehouse-basics.md) — Lakehouse, Delta Lake, ACID, time travel, medallion architecture, `MERGE INTO`.
- [`module-2-bi-with-sql.md`](module-2-bi-with-sql.md) — 4 BI KPIs in **SQL** and **PySpark DataFrame**, with the equivalent SSIS data flow listed for each.
- [`module-3-dashboard-and-wrapup.md`](module-3-dashboard-and-wrapup.md) — AI/BI Dashboard, AI/BI Genie, Unity Catalog walkthrough, take-home assignments.
- [`notebooks/`](notebooks/) — **drop-in `.ipynb` files** for direct upload to Databricks Free Edition (one per module). See [`notebooks/README.md`](notebooks/README.md) for upload instructions.
- Appendices:
  - [`appendix-ssis-to-databricks.md`](appendix-ssis-to-databricks.md) — full Rosetta stone + a 5-step `.dtsx` migration recipe.
  - [`appendix-pyspark-local.md`](appendix-pyspark-local.md) — reproduce the entire workshop offline with vanilla PySpark via `uv`.
  - [`appendix-unity-catalog.md`](appendix-unity-catalog.md) — governance reference card.

## Quickest path for students

If you just want to run the workshop on your Free Edition account without reading the book first:

1. Open [`notebooks/README.md`](notebooks/README.md) and follow **Option A** (import each `.ipynb` from its raw GitHub URL).
2. Run them in order: `00_setup.ipynb` → `01_lakehouse_basics.ipynb` → `02_bi_with_sql.ipynb` → `03_gold_views.ipynb`.
3. Build the AI/BI Dashboard and AI/BI Genie space following [`module-3-dashboard-and-wrapup.md`](module-3-dashboard-and-wrapup.md).

## Prerequisites for editing / building locally

- **Python 3.11+** (pinned by [`.python-version`](.python-version)) and [`uv`](https://docs.astral.sh/uv/).
- **Node.js 18+** — Jupyter Book v2 wraps [`mystmd`](https://mystmd.org/), which is a Node tool.
- **Java 17 (or 11)** — only required if you intend to run the local PySpark appendix.

## Quickstart — install dependencies

```bash
uv sync
```

This creates a `.venv/` and installs `jupyter-book` v2, JupyterLab, PySpark, pandas, and matplotlib.

## Build the book locally

Live preview with hot reload:

```bash
uv run jupyter book start
```

Or a one-shot static HTML build into `_build/html/`:

```bash
uv run jupyter book build --html
```

```bash
# Open in a browser:
open _build/html/index.html        # macOS
xdg-open _build/html/index.html    # Linux
start _build/html/index.html       # Windows
```

If you don't want to use `uv`, you can install `mystmd` directly with `npm install -g mystmd` and run `myst start` / `myst build --html`.

## Deploy to GitHub Pages

The workflow at [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml)
builds the site with `mystmd` on every push to `main` and publishes to GitHub Pages.

To enable it on a new fork:

1. **Settings → Pages → Source**: pick **GitHub Actions**.
2. Push to `main`. The workflow will build and the URL will appear at
   `Settings → Pages → Visit site` (typically `https://<user>.github.io/databricks_free_intro/`).
3. The workflow sets `BASE_URL: /${{ github.event.repository.name }}`, which is correct
   for project pages. For a user/organisation root site, edit the workflow to set
   `BASE_URL: ''`.

## Audience & teaching note

The course assumes prior exposure to **SQL Server + SSIS + SSMS + SSRS / Power BI**
from UBB's BI track. Every module repeats the relevant rows of the SSIS↔Databricks
Rosetta stone inline as side-by-side examples, so the lakehouse never feels like
a separate planet.

Constraints we deliberately accept:

- **Free Edition only** — no Databricks Connect, no classic clusters, no multi-user
  `GRANT` demos (the workspace has only you on it).
- **Serverless only** — every SQL cell, every dashboard tile, every Genie question
  hits the same serverless SQL warehouse.
- **No notebook execution at build time** — the `myst build --html` step never talks
  to Databricks, so the GitHub Pages build is fast and works offline.
