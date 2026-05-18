from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Date dimension - derived from hire dates, start dates, and end dates",
    cluster_by=["date"]
)
def dim_date():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            F.col("HIRE_DT").alias("date_col")
        )
        .union(
            spark.read.table("bronze_hr_dataset")
            .select(F.col("START_DT").alias("date_col"))
        )
        .union(
            spark.read.table("bronze_hr_dataset")
            .select(F.col("END_DT").alias("date_col"))
        )
        .filter(F.col("date_col").isNotNull())
        .select(
            F.col("date_col").cast("date").alias("date"),
            F.year("date_col").alias("year"),
            F.month("date_col").alias("month"),
            F.quarter("date_col").alias("quarter"),
            F.dayofmonth("date_col").alias("day"),
            F.dayofweek("date_col").alias("day_of_week"),
            F.weekofyear("date_col").alias("week_of_year")
        )
        .distinct()
    )
