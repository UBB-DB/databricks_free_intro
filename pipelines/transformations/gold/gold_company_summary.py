from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold - Company-wide summary metrics (single row)"
)
def gold_company_summary():
    return (
        spark.read.table("fact_employee")
        .agg(
            F.lit("Company Wide").alias("METRIC_LEVEL"),
            F.current_timestamp().alias("SNAPSHOT_TIMESTAMP"),
            F.count("EMP_ID").alias("TOTAL_EMPLOYEES"),
            F.countDistinct("DEPARTMENT").alias("TOTAL_DEPARTMENTS"),
            F.countDistinct("LOCATION").alias("TOTAL_LOCATIONS"),
            F.countDistinct("JOB_TITLE").alias("TOTAL_JOB_TITLES"),
            F.sum("SALARY").alias("TOTAL_PAYROLL"),
            F.avg("SALARY").alias("AVG_SALARY"),
            F.percentile_approx("SALARY", 0.5).alias("MEDIAN_SALARY"),
            F.min("SALARY").alias("MIN_SALARY"),
            F.max("SALARY").alias("MAX_SALARY"),
            F.min("HIRE_DT").alias("EARLIEST_HIRE_DATE"),
            F.max("HIRE_DT").alias("LATEST_HIRE_DATE")
        )
        .withColumn(
            "AVG_SALARY_PER_DEPARTMENT",
            F.col("TOTAL_PAYROLL") / F.col("TOTAL_DEPARTMENTS")
        )
        .withColumn(
            "SALARY_RANGE",
            F.col("MAX_SALARY") - F.col("MIN_SALARY")
        )
    )
