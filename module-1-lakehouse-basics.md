# Module 1 (30 min): Data Lakehouse Basics

## Concept quickstart

A **Data Lakehouse** combines:

- Data lake flexibility (low-cost, many data types)
- Data warehouse reliability (governance, performance, SQL access)

In BI, this means faster analytics with one platform for ingestion, transformation, and reporting.

## Hands-on activity

In a SQL cell, inspect the table:

```sql
USE bi_course;
SELECT * FROM sales_demo;
```

Then run:

```sql
SELECT
  region,
  COUNT(*) AS orders,
  SUM(revenue) AS total_revenue
FROM sales_demo
GROUP BY region
ORDER BY total_revenue DESC;
```

## Discussion prompts

- Which region currently leads in revenue?
- What additional columns would improve BI decision-making?
