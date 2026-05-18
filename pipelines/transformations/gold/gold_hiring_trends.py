from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold - Monthly hiring trends with cumulative metrics",
    cluster_by=["HIRE_YEAR", "HIRE_MONTH"]
)
def gold_hiring_trends():
    return (
        spark.read.table("fact_employee")
        .withColumn("HIRE_YEAR", F.year("HIRE_DT"))
        .withColumn("HIRE_MONTH", F.month("HIRE_DT"))
        .withColumn("HIRE_QUARTER", F.quarter("HIRE_DT"))
        .groupBy("HIRE_YEAR", "HIRE_QUARTER", "HIRE_MONTH", "DEPARTMENT")
        .agg(
            F.count("EMP_ID").alias("HIRES_COUNT"),
            F.avg("SALARY").alias("AVG_STARTING_SALARY"),
            F.countDistinct("JOB_TITLE").alias("UNIQUE_ROLES_HIRED")
        )
        .withColumn(
            "YEAR_MONTH",
            F.concat(
                F.col("HIRE_YEAR"),
                F.lit("-"),
                F.lpad(F.col("HIRE_MONTH"), 2, "0")
            )
        )
    )
