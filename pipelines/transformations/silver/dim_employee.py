from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Employee dimension - employee attributes",
    cluster_by=["EMP_ID"]
)
def dim_employee():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            "EMP_ID",
            "EMP_NM",
            "EMAIL",
            "EDUCATION_LEVEL" 
        )
        .distinct()
    )
