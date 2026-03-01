from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

spark = SparkSession.builder.appName("LotsOfData").getOrCreate()

df = spark.range(1, 5000000)

sales_df = df.withColumn("CustomerID", floor(rand()*100000)) \
    .withColumn("RegionId",  floor(rand()*10)) \
    .withColumn("Amount", floor(rand()*1000))  


data = [(1, "2025-01-01", 100"),
         (1, "2025-01-02", 200),
        (1, "2025-01-03", 150),
        (2, "2025-01-01", 300),
        (5, "2025-01-02", 250)
        ]