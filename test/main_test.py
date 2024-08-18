from src.main import transform_data
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def test_transform_data():
    schema = StructType([ \
    StructField("Order Date",StringType(),True), \
    StructField("Profit",StringType(),True)
  ])
    data = [("2/13/2022", "-$200"), ("11/22/2023", "$1,200"), ("8/17/2022", "$2,200")]

    spark = SparkSession.builder.appName('local[*]').getOrCreate()
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df = spark.createDataFrame(data=data, schema=schema)

    assert dict(df.dtypes)["Order Date"] == "string"
    assert dict(df.dtypes)["Profit"] == "string"
    df = transform_data(df)
    
    assert dict(df.dtypes)["Order Date"] == "date"
    assert dict(df.dtypes)["Profit"] == "float"