# Databricks notebook source
# MAGIC %run ../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../test_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/generator_stages/field_generators

# COMMAND ----------

import random
from datetime import datetime
from pyspark.sql import DataFrame, functions as F


# COMMAND ----------

# region Fixtures

def tidy_meta_df_fixture() -> DataFrame:
  data = [
    ["field1", "CATEGORICAL", "x",  None,                 None, 1.,   None],
    ["field1", "CATEGORICAL", "y",  None,                 None, 2.,   None],
    ["field1", "CATEGORICAL", "z",  None,                 None, 3.,   None],
    ["field2", "CATEGORICAL", "x",  None,                 None, 3.,   None],
    ["field2", "CATEGORICAL", "y",  None,                 None, 2.,   None],
    ["field2", "CATEGORICAL", "z",  None,                 None, 1.,   None],
    ["field3", "DATE",        None, datetime(2022, 1, 1), None, 1.,   None],
    ["field3", "DATE",        None, datetime(2022, 1, 2), None, 2.,   None],
    ["field3", "DATE",        None, datetime(2022, 1, 3), None, 3.,   None],
    ["field4", "DATE",        None, datetime(2022, 1, 1), None, 3.,   None],
    ["field4", "DATE",        None, datetime(2022, 1, 2), None, 2.,   None],
    ["field4", "DATE",        None, datetime(2022, 1, 3), None, 1.,   None],
    ["field5", "DISCRETE",    None, None,                 0.,   1.,   None],
    ["field5", "DISCRETE",    None, None,                 1.,   2.,   None],
    ["field5", "DISCRETE",    None, None,                 2.,   3.,   None],
    ["field6", "DISCRETE",    None, None,                 0.,   3.,   None],
    ["field6", "DISCRETE",    None, None,                 1.,   2.,   None],
    ["field6", "DISCRETE",    None, None,                 2.,   1.,   None],
    ["field7", "CONTINUOUS",  None, None,                 None, None, [float(p) for p in range(1, 101)]],
    ["field8", "CONTINUOUS",  None, None,                 None, None, [float(p) for p in range(2, 202, 2)]],
  ]
  schema = "FIELD_NAME: string, VALUE_TYPE: string, VALUE_STRING: string, VALUE_DATE: date, VALUE_NUMERIC: double, WEIGHT: double, VALUE_NUMERIC_ARRAY: array<double>"
  df = spark.createDataFrame(data, schema)
  return df

# endregion

# COMMAND ----------

# region continuous_field_generator tests

continuous_field_generator_test_suite = FunctionTestSuite()


@continuous_field_generator_test_suite.add_test
def test_generated_values_have_expected_percentile_distribution():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  result_df = continuous_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME")
    .agg(
      F.min("VALUE_NUMERIC").alias("MIN_VALUE"),
      F.expr("approx_percentile(VALUE_NUMERIC, 0.25)").alias("LQ_VALUE"),
      F.expr("approx_percentile(VALUE_NUMERIC, 0.5)").alias("MEDIAN_VALUE"),
      F.expr("approx_percentile(VALUE_NUMERIC, 0.75)").alias("UQ_VALUE"),
      F.max("VALUE_NUMERIC").alias("MAX_VALUE"),
    )
  )
  
  assert result_df.first()  # Check some rows were generated
  
  expected_stats_cols = [
    F.when(F.col("FIELD_NAME") == "field7", F.lit(1.0)  ).when(F.col("FIELD_NAME") == "field8", F.lit(2.0)  ).alias("EXPECTED_MIN_VALUE"),
    F.when(F.col("FIELD_NAME") == "field7", F.lit(25.0) ).when(F.col("FIELD_NAME") == "field8", F.lit(50.0) ).alias("EXPECTED_LQ_VALUE"),
    F.when(F.col("FIELD_NAME") == "field7", F.lit(50.0) ).when(F.col("FIELD_NAME") == "field8", F.lit(100.0)).alias("EXPECTED_MEDIAN_VALUE"),
    F.when(F.col("FIELD_NAME") == "field7", F.lit(75.0) ).when(F.col("FIELD_NAME") == "field8", F.lit(150.0)).alias("EXPECTED_UQ_VALUE"),
    F.when(F.col("FIELD_NAME") == "field7", F.lit(100.0)).when(F.col("FIELD_NAME") == "field8", F.lit(200.0)).alias("EXPECTED_MAX_VALUE"),
  ]
  
  aggregated_result_df = aggregated_result_df.select("*", *expected_stats_cols)
  
  rel_precision = 0.1
  assert columns_approx_equal(aggregated_result_df, "MIN_VALUE",    "EXPECTED_MIN_VALUE",    rel_precision), "Expected minimum was different to actual minimum!"
  assert columns_approx_equal(aggregated_result_df, "LQ_VALUE",     "EXPECTED_LQ_VALUE",     rel_precision), "Expected lower quartile was different to actual lower quartile!"
  assert columns_approx_equal(aggregated_result_df, "MEDIAN_VALUE", "EXPECTED_MEDIAN_VALUE", rel_precision), "Expected median was different to actual median!"
  assert columns_approx_equal(aggregated_result_df, "UQ_VALUE",     "EXPECTED_UQ_VALUE",     rel_precision), "Expected upper quartile was different to actual upper quartile!"
  assert columns_approx_equal(aggregated_result_df, "MAX_VALUE",    "EXPECTED_MAX_VALUE",    rel_precision), "Expected maximum was different to actual maximum!"


continuous_field_generator_test_suite.run()
# endregion

# COMMAND ----------

# region categorical_field_generator tests

categorical_field_generator_test_suite = FunctionTestSuite()


@categorical_field_generator_test_suite.add_test
def test_generated_values_have_expected_frequency_distribution():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  result_df = categorical_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated

  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_STRING")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "x"), 1 / 6)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "y"), 2 / 6)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "z"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "x"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "y"), 2 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "z"), 1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)
  
  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"
  
  
@categorical_field_generator_test_suite.add_test
def test_only_categorical_fields_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()

  result_df = categorical_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated
  
  # Check no fields other than categorical
  distinct_fields_df = result_df.select("FIELD_NAME").distinct()
  expected_categorical_fields = ["field1", "field2"]
  assert distinct_fields_df.filter(~F.col("FIELD_NAME").isin(expected_categorical_fields)).first() is None, "Non categorical fields were generated!"
  
 
@categorical_field_generator_test_suite.add_test
def test_zero_weight_values_not_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  meta_df = meta_df.withColumn("WEIGHT", F.lit(0.))

  result_df = categorical_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.count() == 0, "Values generated with weight equal to zero!"
  
  
@categorical_field_generator_test_suite.add_test
def test_null_values_are_generated_with_expected_frequency():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  # Replace 'z' value with null
  meta_df = meta_df.withColumn("VALUE_STRING", F.when(F.col("VALUE_STRING").isin(["x", "y"]), F.col("VALUE_STRING")))

  result_df = categorical_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_STRING")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "x"), 1 / 6)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "y"), 2 / 6)
    .when((F.col("FIELD_NAME") == "field1") & F.isnull("VALUE_STRING"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "x"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "y"), 2 / 6)
    .when((F.col("FIELD_NAME") == "field2") & F.isnull("VALUE_STRING"), 1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)

  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"

  
categorical_field_generator_test_suite.run()

# endregion

# COMMAND ----------

# region date_field_generator tests

date_field_generator_test_suite = FunctionTestSuite()


@date_field_generator_test_suite.add_test
def test_generated_values_have_expected_frequency_distribution():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()

  result_df = date_field_generator(meta_df, base_df, ["FIELD_NAME"])

  assert result_df.first()  # Check some rows were generated
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_DATE")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_DATE") == datetime(2022, 1, 1)), 1 / 6)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 2 / 6)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_DATE") == datetime(2022, 1, 3)), 3 / 6)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_DATE") == datetime(2022, 1, 1)), 3 / 6)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 2 / 6)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_DATE") == datetime(2022, 1, 3)), 1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)

  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.05), "Some or all frequencies did not equal the expected values"
  


@date_field_generator_test_suite.add_test
def test_only_date_fields_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()

  result_df = date_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated
  
  # Check no fields other than date
  distinct_fields_df = result_df.select("FIELD_NAME").distinct()
  expected_date_fields = ["field3", "field4"]
  assert distinct_fields_df.filter(~F.col("FIELD_NAME").isin(expected_date_fields)).first() is None, "Non date fields were generated!"
  
 
@date_field_generator_test_suite.add_test
def test_zero_weight_values_not_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  meta_df = meta_df.withColumn("WEIGHT", F.lit(0.))

  result_df = date_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.count() == 0, "Values generated with weight equal to zero!"
  
  
@date_field_generator_test_suite.add_test
def test_null_values_are_generated_with_expected_frequency():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  # Replace 'z' value with null
  meta_df = meta_df.withColumn("VALUE_DATE", F.when(F.col("VALUE_DATE").isin([datetime(2022, 1, 1), datetime(2022, 1, 2)]), F.col("VALUE_DATE")))

  result_df = date_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_DATE")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field3") &   (F.col("VALUE_DATE") == datetime(2022, 1, 1)), 1 / 6)
    .when((F.col("FIELD_NAME") == "field3") &   (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 2 / 6)
    .when((F.col("FIELD_NAME") == "field3") & F.isnull("VALUE_DATE"), 3 / 6)
    .when((F.col("FIELD_NAME") == "field4") &   (F.col("VALUE_DATE") == datetime(2022, 1, 1)), 3 / 6)
    .when((F.col("FIELD_NAME") == "field4") &   (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 2 / 6)
    .when((F.col("FIELD_NAME") == "field4") & F.isnull("VALUE_DATE"), 1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)

  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"
  
  
date_field_generator_test_suite.run()

# endregion

# COMMAND ----------

# region discrete_field_generator tests

discrete_field_generator_test_suite = FunctionTestSuite()


@discrete_field_generator_test_suite.add_test
def test_generated_values_have_expected_frequency_distribution():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()

  result_df = discrete_field_generator(meta_df, base_df, ["FIELD_NAME"])

  assert result_df.first()  # Check some rows were generated
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_NUMERIC")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 0.), 1 / 6)
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 1.), 2 / 6)
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 2.), 3 / 6)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 0.), 3 / 6)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 1.), 2 / 6)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 2.), 1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)

  rel_precision = 0.075  # Setting this too small leads to random failures
  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", rel_precision), "Some or all frequencies did not equal the expected values"
  

@discrete_field_generator_test_suite.add_test
def test_only_discrete_fields_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()

  result_df = discrete_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated
  
  # Check no fields other than discrete
  distinct_fields_df = result_df.select("FIELD_NAME").distinct()
  expected_discrete_fields = ["field5", "field6"]
  assert distinct_fields_df.filter(~F.col("FIELD_NAME").isin(expected_discrete_fields)).first() is None, "Non discrete fields were generated!"
  
 
@discrete_field_generator_test_suite.add_test
def test_zero_weight_values_not_generated():
  n_rows = 10
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  meta_df = meta_df.withColumn("WEIGHT", F.lit(0.))

  result_df = discrete_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.count() == 0, "Values generated with weight equal to zero!"
  
  
@discrete_field_generator_test_suite.add_test
def test_null_values_are_generated_with_expected_frequency():
  n_rows = 10000
  base_df = spark.range(n_rows)

  meta_df = tidy_meta_df_fixture()
  
  # Replace 3 value with null
  meta_df = meta_df.withColumn("VALUE_NUMERIC", F.when(F.col("VALUE_NUMERIC").isin([0., 1.]), F.col("VALUE_NUMERIC")))

  result_df = discrete_field_generator(meta_df, base_df, ["FIELD_NAME"])
  
  assert result_df.first()  # Check some rows were generated
  
  aggregated_result_df = (
    result_df
    .groupBy("FIELD_NAME", "VALUE_NUMERIC")
    .agg((F.count(F.lit(1)) / n_rows).alias("FREQUENCY"))
  )

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 0.), 1 / 6)
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 1.), 2 / 6)
    .when((F.col("FIELD_NAME") == "field5") & F.isnull("VALUE_NUMERIC"),      3 / 6)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 0.), 3 / 6)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 1.), 2 / 6)
    .when((F.col("FIELD_NAME") == "field6") & F.isnull("VALUE_NUMERIC"),      1 / 6)
  )

  aggregated_result_df = aggregated_result_df.withColumn("EXPECTED_FREQUENCY", expected_frequency_col)

  assert columns_approx_equal(aggregated_result_df, "FREQUENCY", "EXPECTED_FREQUENCY", 0.1), "Some or all frequencies did not equal the expected values"
  
  
discrete_field_generator_test_suite.run()

# endregion

# COMMAND ----------

# region field_generator tests

field_generator_test_suite = FunctionTestSuite()


@field_generator_test_suite.add_test
def test_generated_df_has_expected_number_of_rows():
  n_expected_rows = random.randint(1, 100)
  base_df = spark.range(n_expected_rows)
  
  meta_df = tidy_meta_df_fixture()
  
  result_df = field_generator(meta_df, base_df, "FIELD_NAME")
  
  n_actual_rows = result_df.count()
  
  assert n_actual_rows == n_expected_rows, f"Expected {n_expected_rows} rows but found {n_actual_rows} in generated dataframe"
  
  
@field_generator_test_suite.add_test
def test_generated_df_has_expected_columns():
  base_df = spark.range(1)
  
  meta_df = tidy_meta_df_fixture()
  
  result_df = field_generator(meta_df, base_df, "FIELD_NAME")
  
  expected_cols = sorted(["id", "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8"])
  actual_cols = sorted(result_df.columns)
  
  assert actual_cols == expected_cols, f"Columns of generated dataframe ({actual_cols}) did not match expectations ({expected_cols})"


field_generator_test_suite.run()

# endregion

# COMMAND ----------

