from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectWDCColumns")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
try:
    spark.read.parquet("file:///input/wdc_full_cleaned_v1").printSchema()
finally:
    spark.stop()
