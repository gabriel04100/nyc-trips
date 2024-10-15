from pyspark.sql import SparkSession


# Using with statement to ensure the Spark session gets closed
with SparkSession.builder.appName("nyc-parquet").getOrCreate() as spark:
    data = spark.read.parquet("./data/yellow_tripdata*.parquet")

    # Display the first few rows of the data
    data.show(5)

    # Print schema of the data
    data.printSchema()

    # Create a temporary view for SQL queries
    data.createOrReplaceTempView("NycTable")

    print(spark.sql("select count(*) as trips from NycTable").show())
