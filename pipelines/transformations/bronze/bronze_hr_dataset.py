from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Bronze layer - raw HR dataset ingestion from workspace.default.hr_dataset"
)
def bronze_hr_dataset():
    return spark.read.table("workspace.default.hr_dataset").withColumnRenamed("EDUCATION LEVEL", "EDUCATION_LEVEL")
