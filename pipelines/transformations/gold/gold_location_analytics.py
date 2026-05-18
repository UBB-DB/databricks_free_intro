from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window   

@dp.materialized_view(
    comment="Gold - Location-based workforce and compensation analytics",
    cluster_by=["LOCATION"]
)
def gold_location_analytics():
    return (
        spark.read.table("fact_employee")
        .groupBy("LOCATION")
        .agg(
            F.count("EMP_ID").alias("EMPLOYEE_COUNT"),
            F.countDistinct("DEPARTMENT").alias("DEPARTMENTS_COUNT"),
            F.countDistinct("JOB_TITLE").alias("JOB_TITLES_COUNT"),
            F.avg("SALARY").alias("AVG_SALARY"),
            F.sum("SALARY").alias("TOTAL_SALARY"),
            F.percentile_approx("SALARY", 0.5).alias("MEDIAN_SALARY"),
            F.stddev("SALARY").alias("SALARY_STDDEV")
        )
        .withColumn(
            "SALARY_COST_RANK",
            F.dense_rank().over(
                Window.orderBy(F.desc("TOTAL_SALARY"))
            )
        )
        .withColumn(
            "HEADCOUNT_RANK",
            F.dense_rank().over(
                Window.orderBy(F.desc("EMPLOYEE_COUNT"))
            )
        )
    )
