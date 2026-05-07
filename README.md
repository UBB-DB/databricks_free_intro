# databricks_free_intro

A minimal Jupyter Book workshop repo for a **2-hour university crash course**:
**Business Intelligence with Data Lakehouse-ing** using **Databricks Free Edition**.

- Databricks Free: <https://www.databricks.com/learn/free-edition>
- Workshop template reference: <https://github.com/jupyter-book/workshop-template/>
- Template docs: <https://jupyter-book.github.io/workshop-template/intro/>

## Contents

- `intro.md` – course overview and 120-minute agenda
- `setup-databricks-free.md` – student setup and reproducible starter dataset
- `module-1-lakehouse-basics.md` – lakehouse fundamentals activity
- `module-2-bi-with-sql.md` – BI-style SQL exercises
- `module-3-dashboard-and-wrapup.md` – dashboard task and wrap-up

## Build locally

```bash
pip install jupyter-book
jupyter book build --site --html
```

Then open:

```text
_build/html/index.html
```
