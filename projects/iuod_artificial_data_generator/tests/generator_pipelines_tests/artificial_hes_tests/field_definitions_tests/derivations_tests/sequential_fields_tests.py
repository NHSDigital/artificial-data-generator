# Databricks notebook source
# MAGIC %run ../imports

# COMMAND ----------

# MAGIC %run ../../../../../notebooks/generator_pipelines/artificial_hes/field_definitions/derivations/sequential_fields

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from datetime import datetime
from itertools import starmap

# COMMAND ----------

test_sort_fields_l2r = FunctionTestSuite()


@test_sort_fields_l2r.add_test
def test_sorts_two_columns_correctly():
  # Inputs
  schema = "DATE_OF_BIRTH: date, DATE_OF_DEATH: date"
  df = spark.createDataFrame([
    [datetime(2021, 1, 1), datetime(2020, 1, 1)],
    [datetime(2019, 1, 1), datetime(2020, 1, 1)],
    [datetime(2019, 1, 1), datetime(2016, 1, 12)],
    [datetime(2019, 2, 1), datetime(2019, 1, 1)],
  ], schema)
  
  # Execute function under test
  sorted_columns = sort_fields_l2r(*df.columns)
  sorted_columns = starmap(lambda field_name, col_spec: col_spec.alias(field_name), sorted_columns)  # Assign aliases to columns
  result_df = df.select(*sorted_columns)
  
  # Define expectations
  expected_df = spark.createDataFrame([
    [datetime(2020, 1, 1), datetime(2021, 1, 1)],
    [datetime(2019, 1, 1), datetime(2020, 1, 1)],
    [datetime(2016, 1, 12), datetime(2019, 1, 1)],
    [datetime(2019, 1, 1), datetime(2019, 2, 1)],
  ], schema)
  
  # Check against expectation
  assert dataframes_equal(result_df, expected_df)
  
  
@test_sort_fields_l2r.add_test
def test_sorts_many_columns_correctly():
  # Inputs
  schema = "DATE_OF_BIRTH: date, DATE_OF_ADMISSION: date, DATE_OF_TREATMENT: date, DATE_OF_DISCHARGE: date"
  df = spark.createDataFrame([
    [datetime(2021, 1, 1), datetime(2020, 1, 1), datetime(2020, 3, 1), datetime(2020, 2, 1)],
    [datetime(2016, 1, 1), datetime(2018, 1, 1), datetime(2018, 3, 5), datetime(2020, 3, 4)],
    [datetime(2021, 1, 1), datetime(2020, 2, 1), datetime(2020, 3, 1), datetime(2020, 4, 1)],
    [datetime(2021, 1, 1), datetime(2020, 1, 1), datetime(2019, 3, 1), datetime(2018, 2, 1)],
  ], schema)
  
  # Execute function under test
  sorted_columns = sort_fields_l2r(*df.columns)
  sorted_columns = starmap(lambda field_name, col_spec: col_spec.alias(field_name), sorted_columns)  # Assign aliases to columns
  result_df = df.select(*sorted_columns)
  
  # Define expectations
  expected_df = spark.createDataFrame([
    [datetime(2020, 1, 1), datetime(2020, 2, 1), datetime(2020, 3, 1), datetime(2021, 1, 1)],
    [datetime(2016, 1, 1), datetime(2018, 1, 1), datetime(2018, 3, 5), datetime(2020, 3, 4)],
    [datetime(2020, 2, 1), datetime(2020, 3, 1), datetime(2020, 4, 1), datetime(2021, 1, 1)],
    [datetime(2018, 2, 1), datetime(2019, 3, 1), datetime(2020, 1, 1), datetime(2021, 1, 1)],
  ], schema)
  
  # Check against expectation
  assert dataframes_equal(result_df, expected_df)

  
@test_sort_fields_l2r.add_test
def test_pushes_nulls_to_right_when_sorting():
  # Inputs
  schema = "DATE_OF_BIRTH: date, DATE_OF_DEATH: date"
  df = spark.createDataFrame([
    [datetime(2021, 1, 1), None],
    [datetime(2019, 1, 1), datetime(2020, 1, 1)],
    [None, None],
    [None, datetime(2019, 1, 1)],
  ], schema)
  
  # Execute function under test
  sorted_columns = sort_fields_l2r(*df.columns)
  sorted_columns = starmap(lambda field_name, col_spec: col_spec.alias(field_name), sorted_columns)  # Assign aliases to columns
  result_df = df.select(*sorted_columns)
  
  # Define expectations
  expected_df = spark.createDataFrame([
    [datetime(2021, 1, 1), None],
    [datetime(2019, 1, 1), datetime(2020, 1, 1)],
    [None, None],
    [datetime(2019, 1, 1), None],
  ], schema)
  
  # Check against expectation
  assert dataframes_equal(result_df, expected_df)
  

@test_sort_fields_l2r.add_test
def test_does_not_affect_unspecified_columns():
  # Inputs
  schema = ["DATE_OF_BIRTH", "DATE_OF_DEATH", "ID", "NAME"]
  df = spark.createDataFrame([
    [datetime(2021, 1, 1), datetime(2020, 1, 1), 0, "name1"],
    [datetime(2019, 1, 1), datetime(2020, 1, 1), 1, "name1"],
    [datetime(2019, 1, 1), datetime(2016, 1, 12), 2, "name2"],
    [datetime(2019, 2, 1), datetime(2019, 1, 1), 3, "name3"],
  ], schema)
  
  # Execute function under test
  sorted_columns = sort_fields_l2r("DATE_OF_BIRTH", "DATE_OF_DEATH")
  sorted_columns = starmap(lambda field_name, col_spec: col_spec.alias(field_name), sorted_columns)  # Assign aliases to columns
  result_df = df.select(*sorted_columns, "ID", "NAME")
  
  # Define expectations
  expected_df = spark.createDataFrame([
    [datetime(2020, 1, 1), datetime(2021, 1, 1), 0, "name1"],
    [datetime(2019, 1, 1), datetime(2020, 1, 1), 1, "name1"],
    [datetime(2016, 1, 12), datetime(2019, 1, 1), 2, "name2"],
    [datetime(2019, 1, 1), datetime(2019, 2, 1), 3, "name3"],
  ], schema)
  
  # Check against expectation
  assert dataframes_equal(result_df, expected_df)

  
test_sort_fields_l2r.run()

# COMMAND ----------
