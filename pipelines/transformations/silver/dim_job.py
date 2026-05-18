from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Job dimension - job titles",
    cluster_by=["JOB_TITLE"]
)
def dim_job():
    return (
        spark.read.table("bronze_hr_dataset")
        .select("JOB_TITLE")
        .distinct()
    )
