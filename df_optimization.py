# ===================================
# DATAFRAME OPTIMIZATION - PYSPARK
# SALTING
# What: Adding randomnes to a key to make the data even distributed.
# Why:  When we joing two tables, if data has skewness issues, the performance will be low.
# How:
# Partition 1: 1M -> It will take a very long time to process it
# Partition 2: 2k
# Partition 3: 6k
# To solve, we use a SALT number (total number to distribute your data) i. e 3
# Partition 1 = S1(1) + S2(1) + S3(1)
# Partition 2 = S1(2) + S2(2) + S3(2)
# Partition 3 = S1(3) + S2(3) + S3(3)
# Final table = S1 = S1(1) + S1(2) + S1(3)
#               S2 = S2(1) + S2(2) + S2(3)
#               S3 = S3(1) + S3(2) + S3(3)
# ===================================
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F


spark = SparkSession.builder.master("local[*]").appName('Salting').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df_uniform = spark.createDataFrame([i for i in range(1000000)], IntegerType())
df_uniform.show(5, False)

(
    df_uniform
    .withColumn("partition", F.spark_partition_id())
    .groupBy("partition")
    .count()
    .orderBy("partition")
    .show(15, False)
)

df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 15, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 10, IntegerType()).repartition(1)
df3 = spark.createDataFrame([3] * 5, IntegerType()).repartition(1)
df_skew = df0.union(df1).union(df2).union(df3)
df_skew.show(5, False)