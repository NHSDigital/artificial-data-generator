# Databricks notebook source
# MAGIC %run ../../test_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/scraper_stages/aggregation/field_summarisers

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

# region Prevent downstream linting highlights
spark = spark
FunctionTestSuite = FunctionTestSuite
field_summariser = field_summariser
columns_equal = columns_equal
columns_approx_equal = columns_approx_equal
with_retry = with_retry
# endregion

# COMMAND ----------

def long_format_df_fixture():
  # Multiplier to the right is the expected weight of each field/value combination
  frequency_field_data = [
    *[["table1", "field1", "CATEGORICAL", "x",   None,                None] for _ in range(49)],
    *[["table1", "field1", "CATEGORICAL", "y",   None,                None] for _ in range(45)],
    *[["table1", "field1", "CATEGORICAL", "z",   None,                None] for _ in range(82)],
    *[["table1", "field1", "CATEGORICAL", None,  None,                None] for _ in range(7)],  # Null value categorical
    *[["table1", "field2", "CATEGORICAL", "x",   None,                None] for _ in range(41)],
    *[["table1", "field2", "CATEGORICAL", "y",   None,                None] for _ in range(67)],
    *[["table1", "field2", "CATEGORICAL", "z",   None,                None] for _ in range(97)],
    *[["table1", "field2", "CATEGORICAL", None,  None,                None] for _ in range(37)],  # Null value categorical
    *[["table1", "field3", "DATE",        None, datetime(2022, 1, 1), None] for _ in range(4)],
    *[["table1", "field3", "DATE",        None, datetime(2022, 1, 2), None] for _ in range(60)],
    *[["table1", "field3", "DATE",        None, datetime(2022, 1, 3), None] for _ in range(76)],
    *[["table1", "field3", "DATE",        None, None,                 None] for _ in range(38)],  # Null value date
    *[["table1", "field4", "DATE",        None, datetime(2022, 1, 1), None] for _ in range(7)],
    *[["table1", "field4", "DATE",        None, datetime(2022, 1, 2), None] for _ in range(37)],
    *[["table1", "field4", "DATE",        None, datetime(2022, 1, 3), None] for _ in range(61)],
    *[["table1", "field4", "DATE",        None, None,                 None] for _ in range(33)],  # Null value date
    *[["table1", "field5", "DISCRETE",    None, None,                 1.  ] for _ in range(69)],
    *[["table1", "field5", "DISCRETE",    None, None,                 2.  ] for _ in range(48)],
    *[["table1", "field5", "DISCRETE",    None, None,                 3.  ] for _ in range(56)],
    *[["table1", "field5", "DISCRETE",    None, None,                 None] for _ in range(46)],  # Null value discrete
    *[["table1", "field6", "DISCRETE",    None, None,                 1.  ] for _ in range(94)],
    *[["table1", "field6", "DISCRETE",    None, None,                 2.  ] for _ in range(67)],
    *[["table1", "field6", "DISCRETE",    None, None,                 3.  ] for _ in range(24)],
    *[["table1", "field6", "DISCRETE",    None, None,                 None] for _ in range(85)],
  ]
  schema = "TABLE_NAME: string, FIELD_NAME: string, VALUE_TYPE: string, VALUE_STRING: string, VALUE_DATE: date, VALUE_NUMERIC: double"
  frequency_long_df = spark.createDataFrame(frequency_field_data, schema)
  
  continuous_long_df = (
    spark.range(10000)
    .withColumn("TABLE_NAME", F.lit("table1"))
    .withColumn("FIELD_NAME", F.when(F.rand() > 0.5, "field7").otherwise("field8"))
    .withColumn("VALUE_TYPE", F.lit("CONTINUOUS"))
    .withColumn("VALUE_STRING", F.lit(None).cast("string"))
    .withColumn("VALUE_DATE", F.lit(None).cast("date"))
    .withColumn("VALUE_NUMERIC", F.when(F.col("FIELD_NAME") == "field7", F.rand() * 100).otherwise(F.rand() * 200))
    .drop("id")
  )
  
  return frequency_long_df.unionByName(continuous_long_df)

# COMMAND ----------

field_summariser_test_suite = FunctionTestSuite()


@field_summariser_test_suite.add_test
def test_correct_frequencies_for_categorical_fields():
  input_df = long_format_df_fixture()
  result_df = field_summariser(input_df, 1, 1).filter(F.col("VALUE_TYPE") == "CATEGORICAL")
  
  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "x"), 49.)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "y"), 45.)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "z"), 82.)
    .when((F.col("FIELD_NAME") == "field1") & F.isnull("VALUE_STRING"),       7.)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "x"), 41.)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "y"), 67.)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING") == "z"), 97.)
    .when((F.col("FIELD_NAME") == "field2") & F.isnull("VALUE_STRING"),       37.)
  )
  
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_frequency_col)
  
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weights for categorical fields did not match expectations"
  assert result_df.filter(F.col("VALUE_TYPE") == "CATEGORICAL").count() == 8, "Too many rows found for categorical fields"
  
  
@field_summariser_test_suite.add_test
def test_correct_frequencies_for_date_fields():
  input_df = long_format_df_fixture()
  result_df = field_summariser(input_df, 1, 1).filter(F.col("VALUE_TYPE") == "DATE")
  
  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_DATE") == datetime(2022, 1, 1)), 4.)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 60.)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_DATE") == datetime(2022, 1, 3)), 76.)
    .when((F.col("FIELD_NAME") == "field3") & F.isnull("VALUE_DATE"),                        38.)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_DATE") == datetime(2022, 1, 1)), 7.)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 37.)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_DATE") == datetime(2022, 1, 3)), 61.)
    .when((F.col("FIELD_NAME") == "field4") & F.isnull("VALUE_DATE"),                        33.)
  )
  
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_frequency_col)
  
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weights for date fields did not match expectations"
  assert result_df.filter(F.col("VALUE_TYPE") == "DATE").count() == 8, "Too many rows found for date fields"
  
  
@field_summariser_test_suite.add_test
def test_correct_frequencies_for_discrete_fields():
  input_df = long_format_df_fixture()
  result_df = field_summariser(input_df, 1, 1).filter(F.col("VALUE_TYPE") == "DISCRETE")
  
  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 1), 69.)
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 2), 48.)
    .when((F.col("FIELD_NAME") == "field5") & (F.col("VALUE_NUMERIC") == 3), 56.)
    .when((F.col("FIELD_NAME") == "field5") & F.isnull("VALUE_NUMERIC"),     46.)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 1), 94.)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 2), 67.)
    .when((F.col("FIELD_NAME") == "field6") & (F.col("VALUE_NUMERIC") == 3), 24.)
    .when((F.col("FIELD_NAME") == "field6") & F.isnull("VALUE_NUMERIC"),     85.)
  )
  
  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_frequency_col)
  
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weights for discrete fields did not match expectations"
  assert result_df.filter(F.col("VALUE_TYPE") == "DISCRETE").count() == 8, "Too many rows found for discrete fields"
  
  
@field_summariser_test_suite.add_test
def test_value_field_populated_according_to_value_type():
  input_df = long_format_df_fixture()
  result_df = field_summariser(input_df, 1, 1)
  
  row_where_wrong_field_populated = (
    result_df
    .filter(
      F.expr("VALUE_TYPE == 'CATEGORICAL'  AND (isnotnull(VALUE_DATE)    OR isnotnull(VALUE_NUMERIC) OR isnotnull(VALUE_STRING_ARRAY) OR isnotnull(VALUE_NUMERIC_ARRAY))")
      | F.expr("VALUE_TYPE == 'DATE'       AND (isnotnull(VALUE_NUMERIC) OR isnotnull(VALUE_STRING)  OR isnotnull(VALUE_STRING_ARRAY) OR isnotnull(VALUE_NUMERIC_ARRAY))")
      | F.expr("VALUE_TYPE == 'DISCRETE'   AND (isnotnull(VALUE_STRING)  OR isnotnull(VALUE_DATE)    OR isnotnull(VALUE_STRING_ARRAY) OR isnotnull(VALUE_NUMERIC_ARRAY))")
      | F.expr("VALUE_TYPE == 'CONTINUOUS' AND (isnotnull(VALUE_STRING)  OR isnotnull(VALUE_DATE)    OR isnotnull(VALUE_NUMERIC)      OR isnotnull(VALUE_STRING_ARRAY))" )
    )
    .first()
  )
  
  assert row_where_wrong_field_populated is None
  
  
@field_summariser_test_suite.add_test
def test_frequencies_are_rounded_to_the_nearest_given_number():
  input_df = long_format_df_fixture()
  
  for frequency_rounding in [1, 5, 10]:
    result_df = field_summariser(input_df, frequency_rounding, 0)

    result_df = result_df.withColumn("REMAINDER", F.col("WEIGHT") % frequency_rounding)
    assert result_df.first() is not None  # Some rows

    # No rows with non-zero remainder
    assert result_df.filter("REMAINDER != 0.0").first() is None, f"Frequencies were not rounded to the nearest {frequency_rounding}"
  
  
@field_summariser_test_suite.add_test
def test_values_are_excluded_with_frequency_below_threshold():
  input_df = long_format_df_fixture()
  
  for min_frequency in [0, 5, 10]:
    result_df = field_summariser(input_df, 1, min_frequency)
    assert result_df.first() is not None  # Some rows

    # No rows with weight below threshold
    assert result_df.filter(F.col("WEIGHT") < min_frequency).first() is None, f"Found frequencies below minimum {min_frequency}"
    
    
@field_summariser_test_suite.add_test
def test_values_are_excluded_with_frequency_below_threshold_within_rounding():
  # Need to make sure frequencies aren't rounded up so that they pass the threshold!
  
  data = [
    *[["table1", "field1", "CATEGORICAL", "x",   None,                None] for _ in range(7)],  # Excluded
    *[["table1", "field1", "CATEGORICAL", "y",   None,                None] for _ in range(10)], # Included
    *[["table1", "field2", "DATE",        None, datetime(2022, 1, 1), None] for _ in range(8)],  # Excluded
    *[["table1", "field2", "DATE",        None, datetime(2022, 1, 2), None] for _ in range(13)], # Included
    *[["table1", "field3", "DISCRETE",    None, None,                 1.  ] for _ in range(9)],  # Excluded
    *[["table1", "field3", "DISCRETE",    None, None,                 2.  ] for _ in range(16)], # Included
  ]
  schema = "TABLE_NAME: string, FIELD_NAME: string, VALUE_TYPE: string, VALUE_STRING: string, VALUE_DATE: date, VALUE_NUMERIC: double"
  input_df = spark.createDataFrame(data, schema)
 
  frequency_rounding = 5
  min_frequency = 10
  result_df = field_summariser(input_df, frequency_rounding, min_frequency)
  
  expected_exclusions = (
    F
    .when(F.col("FIELD_NAME") == "field1", F.col("VALUE_STRING") == "x")
    .when(F.col("FIELD_NAME") == "field2", F.col("VALUE_DATE") == datetime(2022, 1, 1))
    .when(F.col("FIELD_NAME") == "field3", F.col("VALUE_NUMERIC") == 1)
    .otherwise(False)
  )

  assert result_df.filter(expected_exclusions).first() is None, "Found values that should have been suppressed!"
  assert result_df.count() == 3, "Too many rows found"

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING") == "y"), 10.)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_DATE") == datetime(2022, 1, 2)), 15.)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_NUMERIC") == 2.), 15.)
  )

  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_frequency_col)
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weights did not match expectations"
        

@field_summariser_test_suite.add_test
@with_retry(n_retries=10)
def test_correct_percentiles_for_continuous_fields():
  input_df = long_format_df_fixture()
  
  result_df = field_summariser(input_df, 1, 1)
  
  expected_percentile_col = (
    F
    .when(F.col("FIELD_NAME") == "field7", F.sequence(F.lit(2), F.lit(99), F.lit(1)).cast("array<double>"))
    .when(F.col("FIELD_NAME") == "field8", F.sequence(F.lit(4), F.lit(198), F.lit(2)).cast("array<double>"))
  )
  result_df = (
    result_df
    .withColumn("EXPECTED_VALUE_NUMERIC_ARRAY", expected_percentile_col)
    .withColumn("PAIRED_ACTUAL_EXPECTED", F.expr("zip_with(VALUE_NUMERIC_ARRAY, EXPECTED_VALUE_NUMERIC_ARRAY, (x, y) -> struct(x as ACTUAL, y as EXPECTED))"))
    .select("FIELD_NAME", F.expr("inline(PAIRED_ACTUAL_EXPECTED)"))
  )
  
  assert columns_approx_equal(result_df, "EXPECTED", "ACTUAL", 0.1), "Difference between ACTUAL and EXPECTED column values"

  
field_summariser_test_suite.run()