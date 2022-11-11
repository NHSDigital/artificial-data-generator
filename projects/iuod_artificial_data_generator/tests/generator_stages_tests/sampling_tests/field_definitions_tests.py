# Databricks notebook source
# MAGIC %run ../../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../test_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/generator_stages/sampling/field_definitions

# COMMAND ----------

import random
from pyspark.sql import functions as F

# COMMAND ----------

# region total_over_window tests

total_over_window_test_suite = FunctionTestSuite()


@total_over_window_test_suite.add_test
def test_correctly_computes_total_within_partition():
  input_data = [
    ["x", 1],
    ["y", 2],
    ["z", 3],
    ["x", 4],
    ["y", 5],
    ["z", 6],
    ["x", 7],
    ["y", 8],
    ["z", 9],
  ]
  input_schema = "FIELD_NAME: string, VALUE: integer"
  input_df = spark.createDataFrame(input_data, input_schema)
  
  sum_col = "VALUE"
  partition_cols = ["FIELD_NAME"]
  result_df = input_df.withColumn("TOTAL", total_over_window(sum_col, partition_cols))
  
  expected_total_col = (
    F
    .when(F.col("FIELD_NAME") == "x", 1 + 4 + 7)
    .when(F.col("FIELD_NAME") == "y", 2 + 5 + 8)
    .when(F.col("FIELD_NAME") == "z", 3 + 6 + 9)
    .cast("long")
  )
  
  result_df = result_df.withColumn("EXPECTED_TOTAL", expected_total_col)
  
  assert columns_equal(result_df, "TOTAL", "EXPECTED_TOTAL"), "Total sum did not equal expected value"


total_over_window_test_suite.run()

# endregion

# COMMAND ----------

# region cumsum_over_window tests

cumsum_over_window_test_suite = FunctionTestSuite()


@cumsum_over_window_test_suite.add_test
def test_correctly_computes_cumsum_over_partition():
  # Column 1 specifies partition
  # Column 2 specifies ordering within the partition
  # Column 3 is the value to cumsum
  input_data = [
    ["x", 1, 1],
    ["y", 1, 2],
    ["z", 1, 3],
    ["x", 2, 4],
    ["y", 2, 5],
    ["z", 2, 6],
    ["x", 3, 7],
    ["y", 3, 8],
    ["z", 3, 9],
  ]
  random.shuffle(input_data)
  input_schema = "PARTITION_FIELD: string, ORDER_FIELD: integer, VALUE: long"
  input_df = spark.createDataFrame(input_data, input_schema)
  
  sum_col = "VALUE"
  partition_cols = "PARTITION_FIELD",
  order_cols = "ORDER_FIELD",
  result_df = input_df.withColumn("CUMSUM", cumsum_over_window(sum_col, partition_cols, order_cols))
  
  # Order expected by partition_key, expected cumulative sum value
  expected_cumsum_col = (
    F
    .when((F.col("PARTITION_FIELD") == "x") & (F.col("ORDER_FIELD") == 1), 1)
    .when((F.col("PARTITION_FIELD") == "x") & (F.col("ORDER_FIELD") == 2), 1 + 4)
    .when((F.col("PARTITION_FIELD") == "x") & (F.col("ORDER_FIELD") == 3), 1 + 4 + 7)
    .when((F.col("PARTITION_FIELD") == "y") & (F.col("ORDER_FIELD") == 1), 2)
    .when((F.col("PARTITION_FIELD") == "y") & (F.col("ORDER_FIELD") == 2), 2 + 5)
    .when((F.col("PARTITION_FIELD") == "y") & (F.col("ORDER_FIELD") == 3), 2 + 5 + 8)
    .when((F.col("PARTITION_FIELD") == "z") & (F.col("ORDER_FIELD") == 1), 3)
    .when((F.col("PARTITION_FIELD") == "z") & (F.col("ORDER_FIELD") == 2), 3 + 6)
    .when((F.col("PARTITION_FIELD") == "z") & (F.col("ORDER_FIELD") == 3), 3 + 6 + 9)
    .cast("long")
  )
  
  result_df = result_df.withColumn("EXPECTED_CUMSUM", expected_cumsum_col)
  
  assert columns_equal(result_df, "CUMSUM", "EXPECTED_CUMSUM"), "Cumulative sum did not equal expected value"

  
cumsum_over_window_test_suite.run()

# endregion

# COMMAND ----------

