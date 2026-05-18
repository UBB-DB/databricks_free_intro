from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Employee fact table - contains employee measures and foreign keys to dimensions",
    cluster_by=["EMP_ID", "DEPARTMENT"]
)
def fact_employee():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            "EMP_ID",
            "JOB_TITLE",
            "DEPARTMENT",
            "LOCATION",
            F.col("HIRE_DT").cast("date").alias("HIRE_DT"),
            F.col("START_DT").cast("date").alias("START_DT"),
            F.col("END_DT").cast("date").alias("END_DT"),
            "SALARY"
        )
    )
