from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Location dimension - location hierarchy",
    cluster_by=["LOCATION"]
)
def dim_location():
    return (
        spark.read.table("bronze_hr_dataset")
        .select(
            "LOCATION",
            "ADDRESS",
            "CITY",
            "STATE"
        )
        .distinct()
    )
