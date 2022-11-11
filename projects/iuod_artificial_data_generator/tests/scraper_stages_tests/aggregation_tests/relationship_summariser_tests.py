# Databricks notebook source
# MAGIC %run ../../test_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/scraper_stages/aggregation/relationship_summariser

# COMMAND ----------

from pyspark.sql import functions as F


summarise_relationships_test_suite = FunctionTestSuite()


@summarise_relationships_test_suite.add_test
def test_1_to_1():
  data = [
    ["a", "1"],
    ["b", "2"],
    ["c", "3"],
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  expected_weight_col = (
    F
    .when(F.col("VALUE_NUMERIC") == 1, 3)
    .cast("double")
  )
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_weight_col)
  
  n_expected_rows = 1
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weight was different to expectation"
  

@summarise_relationships_test_suite.add_test
def test_1_to_1_duplicated():
  data = [
    ["a", "1"],
    ["a", "1"],
    ["b", "2"],
    ["b", "2"],
    ["c", "3"],
    ["c", "3"],
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  expected_weight_col = (
    F
    .when(F.col("VALUE_NUMERIC") == 1, 3)
    .cast("double")
  )
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_weight_col)
  
  n_expected_rows = 1
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weight was different to expectation"
  
  
@summarise_relationships_test_suite.add_test
def test_1_to_many():
  data = [
    ["a", "1"],
    ["b", "2"],
    ["c", "2"],
    ["d", "3"],
    ["e", "3"],
    ["f", "3"],
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  expected_weight_col = (
    F
    .when(F.col("VALUE_NUMERIC") == 1, 1)
    .when(F.col("VALUE_NUMERIC") == 2, 1)
    .when(F.col("VALUE_NUMERIC") == 3, 1)
    .cast("double")
  )
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_weight_col)
  
  n_expected_rows = 3
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weight was different to expectation"
  
  
@summarise_relationships_test_suite.add_test
def test_1_to_many_duplicated():
  data = [
    ["a", "1"],
    ["a", "1"],
    ["b", "2"],
    ["b", "2"],
    ["c", "2"],
    ["c", "2"],
    ["d", "3"],
    ["d", "3"],
    ["e", "3"],
    ["e", "3"],
    ["f", "3"],
    ["f", "3"],
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  expected_weight_col = (
    F
    .when(F.col("VALUE_NUMERIC") == 1, 1)
    .when(F.col("VALUE_NUMERIC") == 2, 1)
    .when(F.col("VALUE_NUMERIC") == 3, 1)
    .cast("double")
  )
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_weight_col)
  
  n_expected_rows = 3
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weight was different to expectation"
  
  
@summarise_relationships_test_suite.add_test
def test_null_primary_keys_give_0_to_1_cardinality_mappings():
  data = [
    [None, "1"]
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  expected_weight_col = (
    F
    .when(F.col("VALUE_NUMERIC") == 0, 1)
    .cast("double")
  )
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_weight_col)
  
  n_expected_rows = 1
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weight was different to expectation"
  
  
@summarise_relationships_test_suite.add_test
def test_null_foreign_keys_are_ignored():
  data = [
    ["a", None]
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  assert result_df.first() is None, "Found rows in the resulting dataframe but expected there to be 0"
  

@summarise_relationships_test_suite.add_test
def test_key_field_names_stored_as_literals_in_metadata():
  data = [
    ["a", "1"]
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  result_df = (
    result_df
    .withColumn("EXPECTED_FIELD_NAME", F.lit("PRIMARY_KEY"))
    .withColumn("EXPECTED_VALUE_STRING", F.lit("FOREIGN_KEY"))
  )
  
  n_expected_rows = 1
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "EXPECTED_FIELD_NAME", "FIELD_NAME")
  assert columns_equal(result_df, "EXPECTED_VALUE_STRING", "VALUE_STRING")
  

@summarise_relationships_test_suite.add_test
def test_key_field_values_excluded_from_metadata():
  data = []
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  assert "PRIMARY_KEY" not in result_df.columns
  assert "FOREIGN_KEY" not in result_df.columns
  

@summarise_relationships_test_suite.add_test
def test_many_to_1_give_1_to_1_cardinality_mappings():
  data = [
    ["1", "a"],
    ["2", "b"],
    ["2", "c"],
    ["3", "d"],
    ["3", "e"],
    ["3", "f"],
  ]
  schema = "PRIMARY_KEY: string, FOREIGN_KEY: string"
  input_df = spark.createDataFrame(data, schema)
  input_df = input_df.withColumn("TABLE_NAME", F.lit("test"))
  
  result_df = summarise_relationships(input_df, "PRIMARY_KEY", "FOREIGN_KEY")
  
  expected_weight_col = (
    F
    .when(F.col("VALUE_NUMERIC") == 1, 6)
    .cast("double")
  )
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_weight_col)
  
  n_expected_rows = 1
  assert result_df.limit(n_expected_rows + 1).count() == n_expected_rows, f"Expected {n_expected_rows} rows but found additional rows"
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weight was different to expectation"

  
summarise_relationships_test_suite.run()

# COMMAND ----------

