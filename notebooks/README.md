# Notebooks — drop-in for Databricks Free Edition

Four self-contained Jupyter notebooks that mirror the workshop. Run them
top-to-bottom on [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
and you have completed every hands-on exercise of the 2-hour course.

| File | Pairs with | Time |
| --- | --- | --- |
| [`00_setup.ipynb`](00_setup.ipynb)                    | [`setup-databricks-free.md`](../setup-databricks-free.md)         | 5 min |
| [`01_lakehouse_basics.ipynb`](01_lakehouse_basics.ipynb) | [`module-1-lakehouse-basics.md`](../module-1-lakehouse-basics.md) | 30 min |
| [`02_bi_with_sql.ipynb`](02_bi_with_sql.ipynb)        | [`module-2-bi-with-sql.md`](../module-2-bi-with-sql.md)           | 40 min |
| [`03_gold_views.ipynb`](03_gold_views.ipynb)          | [`module-3-dashboard-and-wrapup.md`](../module-3-dashboard-and-wrapup.md) | 5 min (then UI work) |

## How to upload to Databricks Free Edition

You have three equivalent options.

### Option A — Import each `.ipynb` from the GitHub raw URL

1. In your Databricks workspace, open **Workspace → Home → ⋮ → Import**.
2. Choose **URL**, paste the raw GitHub URL of one of the notebooks, e.g.
   `https://raw.githubusercontent.com/thec0dewriter/databricks_free_intro/main/notebooks/00_setup.ipynb`.
3. Repeat for the other three.

### Option B — Drag and drop a downloaded `.ipynb`

1. Clone the repo or download the four `.ipynb` files from
   <https://github.com/thec0dewriter/databricks_free_intro/tree/main/notebooks>.
2. In your Databricks workspace, **Workspace → Home → ⋮ → Import → File**, and
   drop the file in.

### Option C — Use the Git folder integration

1. **Workspace → Repos → Add repo**, paste
   `https://github.com/thec0dewriter/databricks_free_intro` and click *Create*.
2. The `notebooks/` folder appears as a synchronised Git folder; open any
   notebook from there.

```{tip}
After import, attach each notebook to **Serverless** compute (top-right of
the notebook bar). The serverless SQL warehouse only needs to be running
for SQL Editor / dashboard work; notebook cells (`%sql` and Python) run on
serverless notebook compute.
```

## Run order

Do them in this order — each notebook expects state created by the previous
one:

1. **`00_setup.ipynb`** — creates `workspace.bi_course` schema and the three
   sample Delta tables.
2. **`01_lakehouse_basics.ipynb`** — exercises ACID, time travel, and
   `MERGE INTO` (SCD Type 1) on the sample tables.
3. **`02_bi_with_sql.ipynb`** — runs the four BI KPIs in **SQL** and in
   **PySpark DataFrame** form.
4. **`03_gold_views.ipynb`** — promotes the KPIs to governed `v_kpi_*` views
   that the AI/BI Dashboard in Module 3 binds to.

## Notebook conventions

- **Default cell language:** Python. SQL cells use the `%sql` magic at the
  top — this is the canonical Databricks pattern and works without any
  cell-level metadata changes.
- **Three-part names everywhere:** every table is referenced as
  `workspace.bi_course.<table>` so the notebooks work on any Unity Catalog
  workspace, not only Free Edition.
- **No external dependencies:** every cell uses only what is already
  available on Free Edition's serverless runtime.

## Regenerating the notebooks

The `.ipynb` files are generated from a single Python source of truth
([`_build_notebooks.py`](_build_notebooks.py)) so JSON escaping never has to
be done by hand. To regenerate after editing:

```bash
python notebooks/_build_notebooks.py
```

The script has zero third-party dependencies — only the Python standard
library — so you do not need to `uv sync` first.

## Running the same notebooks locally

These notebooks are written for the **Databricks Free Edition runtime** —
they assume `spark` is pre-bound and that Unity Catalog tables under
`workspace.bi_course` exist. To execute them outside Databricks, follow
[`appendix-pyspark-local.md`](../appendix-pyspark-local.md) instead, which
ships the equivalent code adapted to read from local CSV files.
