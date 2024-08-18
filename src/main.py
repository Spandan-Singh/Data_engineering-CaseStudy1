from sys import argv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month, to_date, regexp_replace, sum

# filepath_sales = './dataset/Global Superstore Sales - Global Superstore Sales.csv'
# filepath_returns = './dataset/Global Superstore Sales - Global Superstore Returns.csv'

def transform_data(df_filtered_sales: DataFrame) -> DataFrame:
    # Casting fields
    return df_filtered_sales\
                        .withColumn("Order Date", to_date(df_filtered_sales["Order Date"], "MM/dd/yyyy"))\
                        .withColumn("Profit", regexp_replace("Profit", "\\$", ""))\
                        .withColumn("Profit", regexp_replace("Profit", ",", "").cast("float"))

def create_year_month(df_filtered_sales: DataFrame) -> DataFrame:
    # Creating year and month fields                   
    return df_filtered_sales.withColumn("Year", year(df_filtered_sales["Order Date"]))\
                        .withColumn("Month", month(df_filtered_sales["Order Date"]))
    


def main(filepath_sales, filepath_returns):
    spark = SparkSession.builder.appName("case_study_1").master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df_sales :DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath_sales)
    df_returns :DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath_returns)
    
    # TODO: For the total profit and total quantity calculations the returns data should be used to exclude all returns
    # Filter out the the data matching in retuns for Sale file
    df_filtered_sales :DataFrame = df_sales.join(df_returns, ["Order ID"], "leftanti")

    df_filtered_sales = transform_data(df_filtered_sales)
    df_filtered_sales = create_year_month(df_filtered_sales)

    # TODO: That contains the aggregate (sum) Profit and Quantity, based on Year, Month, Category, Sub Category
    # TODO: Year, Month, Category, Sub Category, Total Quantity Sold, Total Profit
    df_filtered_sales = df_filtered_sales.groupBy(["Year", "Month", "Category", "Sub-Category"])\
                                            .agg(sum("Profit").alias("Total Profit"),
                                            sum("Quantity").alias("Total Quantity Sold"))
    
    # TODO: This data has to be stored in a partition based on year month. like year=2014/month=11
    df_filtered_sales.coalesce(1)\
        .write.partitionBy(["Year", "Month"]).mode("overwrite").format("csv")\
        .option("header", "true")\
        .save("./dataset/case_study_report.csv")\


# TODO: UNIT TEST
if __name__ == '__main__':
    # TODO: Inputs should be sales and return files
    _, filepath_sales, filepath_returns = argv
    main(filepath_sales, filepath_returns)  