# Databricks notebook source
# MAGIC %run ../test_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/generator_stages/relationship_generator

# COMMAND ----------

import random

from pyspark.sql import functions as F, types as T

# COMMAND ----------

# region Fixtures

def tidy_meta_df_fixture() -> DataFrame:
  data = [
    ["field1", "RELATIONSHIP", 1., 1., None, None],
    ["field1", "RELATIONSHIP", 2., 2., None, None],
    ["field1", "RELATIONSHIP", 3., 3., None, None],
    ["field2", "RELATIONSHIP", 4., 3., None, None],
    ["field2", "RELATIONSHIP", 5., 2., None, None],
    ["field2", "RELATIONSHIP", 6., 1., None, None],
    ["field3", "OTHER",        7., 3., None, None],
    ["field3", "OTHER",        8., 2., None, None],
    ["field3", "OTHER",        9., 1., None, None],
  ]
  schema = "FIELD_NAME: string, VALUE_TYPE: string, VALUE_NUMERIC: double, WEIGHT: double, VALUE_STRING: string, VALUE_DATE: date"
  df = spark.createDataFrame(data, schema)
  return df

# endregion

# COMMAND ----------

# region relationship_generator tests

relationship_generator_test_suite = FunctionTestSuite()


@relationship_generator_test_suite.add_test
def test_generated_values_have_expected_frequency_distribution():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  result_df = relationship_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first() is not None, "Result did not have any rows"
  
  # Check size of each array
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "id")
    .agg(F.count(F.lit(1)).alias("CARDINALITY"))
    .groupBy("FIELD_NAME", "CARDINALITY")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("CARDINALITY") == 1), 1 / 6)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("CARDINALITY") == 2), 2 / 6)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("CARDINALITY") == 3), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("CARDINALITY") == 4), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("CARDINALITY") == 5), 2 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("CARDINALITY") == 6), 1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)
  
  n_expected_rows = 6
  assert aggregated_result_df.limit(n_expected_rows+1).count() == n_expected_rows, "Fewer that expected rows in the aggregated result"

  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"
  
  
@relationship_generator_test_suite.add_test  
def test_only_relationship_fields_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()

  result_df = relationship_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first() is not None, "Result did not have any rows"
  
  # Check no fields other than categorical
  distinct_fields_df = result_df.select("FIELD_NAME").distinct()
  expected_relationship_fields = ["field1", "field2"]
  assert distinct_fields_df.filter(~F.col("FIELD_NAME").isin(expected_relationship_fields)).first() is None, "Non relationship fields were generated!"
  
  
@relationship_generator_test_suite.add_test
def test_zero_weight_values_not_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  meta_df = meta_df.withColumn("WEIGHT", F.lit(0.))

  result_df = relationship_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first() is None, "Values generated with weight equal to zero!"
  
  
@relationship_generator_test_suite.add_test
def test_zero_values_do_not_have_rows_in_resulting_df():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  meta_df = meta_df.withColumn("VALUE_NUMERIC", F.lit(0.))

  result_df = relationship_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first() is None, "Did not expect any rows to be generated"
  

@relationship_generator_test_suite.add_test
def test_null_values_do_not_have_rows_in_resulting_df():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  meta_df = meta_df.withColumn("VALUE_NUMERIC", F.lit(None).cast("double"))

  result_df = relationship_generator(meta_df, base_df, ["FIELD_NAME"])
    
  assert result_df.first() is None, "Did not expect any rows to be generated"
  
  
@relationship_generator_test_suite.add_test
def test_maximum_generated_cardinality_less_or_equal_output_ratio_threshold():
  # If the cardinality is higher than the outputRatioThreshold, then the pipeline could error
  # when trying to explode the relationships
  output_ratio_threshold = int(spark.conf.get("spark.databricks.queryWatchdog.outputRatioThreshold"))
  # Create metadata with cardinality above the threshold
  data = [["field1", "RELATIONSHIP", output_ratio_threshold ** 2., 1., None, None]]
  schema = "FIELD_NAME: string, VALUE_TYPE: string, VALUE_NUMERIC: double, WEIGHT: double, VALUE_STRING: string, VALUE_DATE: date"
  meta_df = spark.createDataFrame(data, schema)
  
  n_rows = 1
  base_df = spark.range(n_rows)
  
  result_df = relationship_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.count() == output_ratio_threshold
  
  
relationship_generator_test_suite.run()

# endregion

# COMMAND ----------

