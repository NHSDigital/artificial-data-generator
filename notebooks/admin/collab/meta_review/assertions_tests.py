# Databricks notebook source
# MAGIC %run ./assertions

# COMMAND ----------

from unittest import TestCase

test_data = [
  ["table1", "FIELD1", "bad_value1", 4],
  ["table1", "FIELD1", "bad_value2", 7],
  ["table1", "FIELD1", "good_value", 15],
]
test_schema = "TABLE_NAME: string, FIELD_NAME: string, VALUE: string, FREQUENCY: integer"
test_df = spark.createDataFrame(test_data, test_schema)
test_value_col = F.col("VALUE")
test_freq_col = F.col("FREQUENCY")

with TestCase().assertRaises(StatisticalDisclosureControlException):
  assert_statistical_disclosure_controls(test_df.where(F.col("VALUE") == "bad_value1"), test_freq_col, threshold=5, rounding=5)
  
with TestCase().assertRaises(StatisticalDisclosureControlException):
  assert_statistical_disclosure_controls(test_df.where(F.col("VALUE") == "bad_value2"), test_freq_col, threshold=5, rounding=5)
  
assert_statistical_disclosure_controls(test_df.where(F.col("VALUE") == "good_value"), test_freq_col, threshold=5, rounding=5)