from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold - Department-level aggregated metrics for business reporting",
    cluster_by=["DEPARTMENT"]
)
def gold_department_metrics():
    return (
        spark.read.table("fact_employee")
        .groupBy("DEPARTMENT")
        .agg(
            F.count("EMP_ID").alias("EMPLOYEE_COUNT"),
            F.sum("SALARY").alias("TOTAL_SALARY"),
            F.avg("SALARY").alias("AVG_SALARY"),
            F.min("SALARY").alias("MIN_SALARY"),
            F.max("SALARY").alias("MAX_SALARY"),
            F.percentile_approx("SALARY", 0.5).alias("MEDIAN_SALARY"),
            F.countDistinct("JOB_TITLE").alias("UNIQUE_JOB_TITLES"),
            F.countDistinct("LOCATION").alias("UNIQUE_LOCATIONS"),
            F.min("HIRE_DT").alias("EARLIEST_HIRE_DATE"),
            F.max("HIRE_DT").alias("LATEST_HIRE_DATE")
        )
    )
