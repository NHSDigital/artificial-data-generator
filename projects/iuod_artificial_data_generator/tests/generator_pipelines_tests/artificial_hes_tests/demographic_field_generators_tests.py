# Databricks notebook source
# MAGIC %run ../../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../test_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/generator_pipelines/artificial_hes/demographic_field_generators

# COMMAND ----------

# import demographic_field_generators


from datetime import datetime
from pyspark.sql import DataFrame, functions as F


def tidy_demographic_meta_df_fixture() -> DataFrame:
  data = [
    ["field1", "DEMOGRAPHIC_CATEGORICAL", ["a", "b"],  None,         1., [2., 3.], None],
    ["field1", "DEMOGRAPHIC_CATEGORICAL", ["c", "d"],  None,         2., [1., 1.], None],
    ["field1", "DEMOGRAPHIC_CATEGORICAL", ["e"],       None,         3., [1.],     None],
    ["field2", "DEMOGRAPHIC_CATEGORICAL", ["a"],       None,         3., [1.],     None],
    ["field2", "DEMOGRAPHIC_CATEGORICAL", ["b", "c"],  None,         2., [2., 3.], None],
    ["field2", "DEMOGRAPHIC_CATEGORICAL", ["d", "e"],  None,         1., [1., 1.], None],
    ["field3", "DEMOGRAPHIC_DATE",        None,       "2022-01-01",  1., None,     0.5],
    ["field3", "DEMOGRAPHIC_DATE",        None,       "2022-01-02",  2., None,     1.0],
    ["field3", "DEMOGRAPHIC_DATE",        None,       "2022-01-03",  3., None,     1.0],
    ["field4", "DEMOGRAPHIC_DATE",        None,       "2022-01-01",  3., None,     1.0],
    ["field4", "DEMOGRAPHIC_DATE",        None,       "2022-01-02",  2., None,     0.8],
    ["field4", "DEMOGRAPHIC_DATE",        None,       "2022-01-03",  1., None,     1.0],
  ]
  schema = "FIELD_NAME: string, VALUE_TYPE: string, VALUE_STRING_ARRAY: array<string>, VALUE_STRING: string, WEIGHT: double, WEIGHT_ARRAY: array<double>, VALUE_NUMERIC: double"
  df = spark.createDataFrame(data, schema)
  return df

# COMMAND ----------

demographic_categorical_field_generator_test_suite = FunctionTestSuite()


@demographic_categorical_field_generator_test_suite.add_test
def test_generated_values_have_expected_frequency_distribution():
  n_rows = 10000
  n_array_values = 10
  base_df = (
    spark.range(n_rows)
    .withColumn("id2", F.explode(F.sequence(F.lit(1), F.lit(n_array_values), F.lit(1))))  # 10 values per unique id
  )

  meta_df = tidy_demographic_meta_df_fixture()
  
  result_df = demographic_categorical_field_generator(meta_df, base_df, ["FIELD_NAME"], ["id", "id2"])

  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_STRING")
    .agg((F.count(F.lit(1)) / (n_rows * n_array_values)).alias("FREQUENCY"))
  )
  
  # Need to account for the vector weights too
  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "a"), (2 / 5) * (1 / 6))
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "b"), (3 / 5) * (1 / 6))
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "c"), (1 / 2) * (2 / 6))
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "d"), (1 / 2) * (2 / 6))
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "e"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "a"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "b"), (2 / 5) * (2 / 6))
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "c"), (3 / 5) * (2 / 6))
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "d"), (1 / 2) * (1 / 6))
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "e"), (1 / 2) * (1 / 6))
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)
  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"


demographic_categorical_field_generator_test_suite.run()


# COMMAND ----------

demographic_date_field_generator_test_suite = FunctionTestSuite()


@demographic_date_field_generator_test_suite.add_test
def test_generated_values_have_expected_frequency_distribution():
  n_rows = 10000
  n_array_values = 100
  base_df = (
    spark.range(n_rows)
    .withColumn("id2", F.explode(F.sequence(F.lit(1), F.lit(n_array_values), F.lit(1))))  # n values per unique id
  )

  meta_df = tidy_demographic_meta_df_fixture()
  
  result_df = demographic_date_field_generator(meta_df, base_df, ["FIELD_NAME"], ["id", "id2"])
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_STRING")
    .agg((F.count(F.lit(1)) / (n_rows * n_array_values)).alias("FREQUENCY"))
  )
  
  # Expected frequencies (approx. accounting for the noise incidence)
  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == F.lit("2022-01-01")), 0.5 * (1 / 6))
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == F.lit("2022-01-02")), (2 / 6) + ((2 / 5) * (1 / 24)))
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == F.lit("2022-01-03")), (3 / 6) + ((3 / 5) * (1 / 24)))
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == F.lit("2022-01-01")), (3 / 6) + ((3 / 5) * (1 / 30)))
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == F.lit("2022-01-02")), 0.8 * (2 / 6))
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == F.lit("2022-01-03")), (1 / 6) + ((2 / 5) * (1 / 30)))
  )
  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)
  
  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"


demographic_date_field_generator_test_suite.run()

# COMMAND ----------

