import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Deep App").getOrCreate()

print(spark)