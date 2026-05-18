from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Department dimension - department details with managers",
    cluster_by=["DEPARTMENT"]
)
def dim_department():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            "DEPARTMENT",
            "MANAGER"
        )
        .distinct()
    )
