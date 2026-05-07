# Setup: Databricks Free Edition

## 1) Create an account

- Go to <https://www.databricks.com/learn/free-edition>
- Sign up with your academic or personal email.
- Confirm your account and open your workspace.

## 2) Create your first SQL warehouse

1. Open **SQL Warehouse**.
2. Create a new warehouse (small/default settings are enough).
3. Start it before running SQL queries.

## 3) Create a notebook

1. In **Workspace**, create a new notebook.
2. Add one `%python` cell and one `%sql` cell.
3. Attach the notebook to your running compute/warehouse as needed.

## 4) Prepare a reproducible sample table

Run this in a SQL cell:

```sql
CREATE SCHEMA IF NOT EXISTS bi_course;
USE bi_course;

CREATE OR REPLACE TABLE sales_demo AS
SELECT * FROM VALUES
  (1, 'North', 'Laptop', 1200, '2026-01-15'),
  (2, 'North', 'Mouse', 25, '2026-01-15'),
  (3, 'South', 'Laptop', 1100, '2026-01-16'),
  (4, 'South', 'Keyboard', 80, '2026-01-16'),
  (5, 'West', 'Monitor', 300, '2026-01-17'),
  (6, 'West', 'Mouse', 25, '2026-01-17'),
  (7, 'East', 'Laptop', 1250, '2026-01-18'),
  (8, 'East', 'Monitor', 320, '2026-01-18')
AS t(order_id, region, product, revenue, order_date);
```

You now have a fully reproducible dataset for the rest of the workshop.
