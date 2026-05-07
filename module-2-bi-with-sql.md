# Module 2 (40 min): BI Analysis with SQL

## KPI 1: Revenue by product

```sql
USE bi_course;

SELECT
  product,
  SUM(revenue) AS revenue
FROM sales_demo
GROUP BY product
ORDER BY revenue DESC;
```

## KPI 2: Daily trend

```sql
SELECT
  order_date,
  SUM(revenue) AS daily_revenue
FROM sales_demo
GROUP BY order_date
ORDER BY order_date;
```

## KPI 3: Region and product matrix

```sql
SELECT
  region,
  product,
  SUM(revenue) AS revenue
FROM sales_demo
GROUP BY region, product
ORDER BY region, revenue DESC;
```

## Optional extension (if time allows)

Create a view for BI dashboards:

```sql
CREATE OR REPLACE VIEW v_region_daily_revenue AS
SELECT
  region,
  order_date,
  SUM(revenue) AS daily_revenue
FROM sales_demo
GROUP BY region, order_date;
```
