---
title: Databricks roles and lakehouse overview
short_title: Roles overview
---

# Databricks roles: data engineer, analyst, and ML engineer

This page gives a quick role-based map of how teams collaborate on the
Databricks Data Intelligence platform.

![Databricks Lakehouse reference architecture overview](https://learn.microsoft.com/en-us/azure/databricks/_static/images/lakehouse-architecture/ref-arch-overview-azure.png)

Source: Microsoft Learn reference architecture image.

## Role overview

| Role | Primary focus | Typical Databricks assets | Main outputs |
| --- | --- | --- | --- |
| Data Engineer | Build reliable ingestion and transformation pipelines | Workflows, Delta tables, Unity Catalog objects, expectations | Bronze/Silver/Gold datasets, data contracts, pipeline SLAs |
| Data Analyst | Explore data and deliver business insights | SQL Warehouse, dashboards, SQL queries, Genie spaces | KPI dashboards, ad-hoc analysis, decision-ready reports |
| ML Engineer / Data Scientist | Train, track, and deploy models | Notebooks, MLflow experiments, Feature Store tables, model serving endpoints | Reproducible experiments, model versions, prediction APIs |

## How the roles collaborate

1. Data engineers ingest and model data into medallion layers.
2. Analysts consume governed gold data for BI dashboards and SQL analysis.
3. ML engineers build features from curated data and operationalize models.
4. All roles use Unity Catalog for shared governance, lineage, and permissions.

## Why this matters in this course

- Module 1 and Module 4 map strongly to the data engineer workflow.
- Module 2 and Module 3 map strongly to the analyst workflow.
- The same governed lakehouse foundation can later power ML workflows.

This role view helps students connect familiar SQL/SSIS tasks to modern,
collaborative lakehouse delivery.
