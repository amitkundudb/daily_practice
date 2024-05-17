# Databricks notebook source

# Detecting initial data skew
initial_data = spark.range(1, 1001).repartition(10)
initial_data = initial_data.withColumn("id", initial_data["id"] % 10)
initial_skew_stats = initial_data.rdd.map(lambda row: (row.mod_10, 1)).countByKey()
print("Initial Data Skew Statistics:")
print(initial_skew_stats)

# Mitigating data skew through repartitioning
repartitioned = initial_data.repartition(100)
repartitioned_skew_stats = repartitioned.rdd.map(lambda row: (row.id, 1)).countByKey()
print("Data Distribution After Repartitioning:")
print(repartitioned_skew_stats)

# Mitigating data skew through salting
from pyspark.sql.functions import hash
salted = initial_data.repartition(100, hash("id"))
salted_skew_stats = salted.rdd.map(lambda row: (row.id, 1)).countByKey()
print("Data Distribution After Salting:")
print(salted_skew_stats)

# COMMAND ----------


