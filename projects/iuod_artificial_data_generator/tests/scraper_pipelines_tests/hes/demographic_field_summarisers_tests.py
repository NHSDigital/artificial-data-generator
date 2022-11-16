# Databricks notebook source
# MAGIC %run ../../test_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/scraper_pipelines/hes/demographic_field_summarisers

# COMMAND ----------

import random
from datetime import datetime, timedelta

from pyspark.sql import functions as F


# region Prevent downstream linting highlights
spark = spark
HES_PATIENT_KEY = HES_PATIENT_KEY
demographic_categorical_summariser = demographic_categorical_summariser
demographic_date_summariser = demographic_date_summariser
FunctionTestSuite = FunctionTestSuite
columns_equal = columns_equal
# endregion


def long_format_df_fixture():
  frequency_field_data = [
    # Categorical fields
    ## Field 1
    ### Pattern 1 - 12 patients 1x:1y split
    *[
      *[["table1", "field1", f"patient{i}", "DEMOGRAPHIC_CATEGORICAL", "x", None] for i in range(12) for _ in range(3)],
      *[["table1", "field1", f"patient{i}", "DEMOGRAPHIC_CATEGORICAL", "y", None] for i in range(12) for _ in range(3)],
    ],
    ### Pattern 2 - 6 patients 1x
    *[
      *[["table1", "field1", f"patient{j}", "DEMOGRAPHIC_CATEGORICAL", "x", None] for j in range(12, 18) for _ in range(15)],
    ],
    ### Pattern 3 - 4 patients 1x:3z split
    *[
      *[["table1", "field1", f"patient{k}", "DEMOGRAPHIC_CATEGORICAL", "x", None] for k in range(18, 22) for _ in range(1)],
      *[["table1", "field1", f"patient{k}", "DEMOGRAPHIC_CATEGORICAL", "z", None] for k in range(18, 22) for _ in range(3)],
    ],
    ## Field 2
    ### Pattern 1 - 13 patients 3x:1y split
    *[
      *[["table1", "field2", f"patient{i}", "DEMOGRAPHIC_CATEGORICAL", "x", None] for i in range(13) for _ in range(3)],
      *[["table1", "field2", f"patient{i}", "DEMOGRAPHIC_CATEGORICAL", "y", None] for i in range(13) for _ in range(1)],
    ],
    ### Pattern 2 - 8 patients 1y
    *[
      *[["table1", "field2", f"patient{j}", "DEMOGRAPHIC_CATEGORICAL", "y", None] for j in range(13, 21) for _ in range(7)],
    ],
    ### Pattern 3 - 3 patients 1x:1null split
    *[
      *[["table1", "field2", f"patient{k}", "DEMOGRAPHIC_CATEGORICAL", "x", None] for k in range(21, 24) for _ in range(1)],
      *[["table1", "field2", f"patient{k}", "DEMOGRAPHIC_CATEGORICAL", None, None] for k in range(21, 24) for _ in range(1)],
    ],
    # Date fields
    ## Field 3
    ### Pattern 1 - 12 patients 25% records different from mode
    *[
      *[["table1", "field3", f"patient{i}", "DEMOGRAPHIC_DATE", None, datetime(2020, 1, 1)                                         ] for i in range(12) for _ in range(75)],
      *[["table1", "field3", f"patient{i}", "DEMOGRAPHIC_DATE", None, datetime(2020, 1, 1) + timedelta(days=random.randint(1, 100))] for i in range(12) for _ in range(25)],
    ],
    ### Pattern 2 - 6 patients 0% records different from mode
    *[
      *[["table1", "field3", f"patient{j}", "DEMOGRAPHIC_DATE", None, datetime(2021, 1, 1)                                         ] for j in range(12, 18) for _ in range(11)],
    ],
    ### Pattern 3 - 4 patients 75% records different from mode
    *[
      *[["table1", "field3", f"patient{k}", "DEMOGRAPHIC_DATE", None, datetime(2022, 1, 1)                                         ] for k in range(18, 22) for _ in range(25)],
      *[["table1", "field3", f"patient{k}", "DEMOGRAPHIC_DATE", None, datetime(2022, 1, 1) + timedelta(days=random.randint(1, 100))] for k in range(18, 22) for _ in range(75)],
    ],
    ## Field 4
    ### Pattern 1 - 13 patients 25% records different from mode
    *[
      *[["table1", "field4", f"patient{i}", "DEMOGRAPHIC_DATE", None, datetime(2020, 1, 1)                                         ] for i in range(13) for _ in range(75)],
      *[["table1", "field4", f"patient{i}", "DEMOGRAPHIC_DATE", None, datetime(2020, 1, 1) + timedelta(days=random.randint(1, 100))] for i in range(13) for _ in range(25)],
    ],
    ### Pattern 2 - 8 patients 0% records different from mode
    *[
      *[["table1", "field4", f"patient{j}", "DEMOGRAPHIC_DATE", None, datetime(2021, 1, 1)                                         ] for j in range(13, 21) for _ in range(7)],
    ],
    ### Pattern 3 - 3 patients 50% records different from mode
    *[
      *[["table1", "field4", f"patient{k}", "DEMOGRAPHIC_DATE", None, datetime(2022, 1, 1)                                         ] for k in range(21, 24) for _ in range(50)],
      *[["table1", "field4", f"patient{k}", "DEMOGRAPHIC_DATE", None, datetime(2022, 1, 1) + timedelta(days=random.randint(1, 100))] for k in range(21, 24) for _ in range(50)],
    ],
  ]
  schema = f"TABLE_NAME: string, FIELD_NAME: string, {HES_PATIENT_KEY}: string, VALUE_TYPE: string, VALUE_STRING: string, VALUE_DATE: date"
  frequency_long_df = spark.createDataFrame(frequency_field_data, schema)

  # Format dates as strings
  string_or_date_value_col = F.coalesce(F.col("VALUE_STRING"), F.date_format("VALUE_DATE", "yyyy-MM-dd"))  
  frequency_long_df = (
    frequency_long_df
    .withColumn("VALUE_STRING", string_or_date_value_col)
    .drop("VALUE_DATE")
  )
  
  return frequency_long_df

# COMMAND ----------

demographic_categorical_summariser_test_suite = FunctionTestSuite()

  
@demographic_categorical_summariser_test_suite.add_test
def test_correct_value_weights_for_demographic_categorical_fields():
  input_df = long_format_df_fixture()
  input_df = input_df.unionByName(input_df.withColumn(HES_PATIENT_KEY, F.concat_ws("_", F.col(HES_PATIENT_KEY), F.lit("clone"))))  # Double up to avoid suppression without changing weights
  
  result_df = demographic_categorical_summariser(input_df)
  
  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x', 'y')")),  F.expr("array(0.5, 0.5)"))
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x')")),       F.expr("array(1.0)"))
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x', 'z')")),  F.expr("array(0.3, 0.8)"))  # Actually 0.25, 0.75 but rounded to 1dp
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x', 'y')")),  F.expr("array(0.8, 0.3)"))  # Actually 0.75, 0.25 but rounded to 1dp
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('y')")),       F.expr("array(1.0)"))
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING_ARRAY") == F.expr("array(null, 'x')")), F.expr("array(0.5, 0.5)"))
    .cast("array<double>")
  )
  
  result_df = result_df.withColumn("EXPECTED_WEIGHT_ARRAY", expected_frequency_col)
  assert columns_equal(result_df, "WEIGHT_ARRAY", "EXPECTED_WEIGHT_ARRAY", check_nullability=False), "Weight arrays for demographic categorical fields did not match expectations"
  
  expected_row_count = 6  
  actual_row_count = result_df.filter(F.col("VALUE_TYPE") == "DEMOGRAPHIC_CATEGORICAL").count()
  assert actual_row_count == expected_row_count, f"Found {actual_row_count} rows but expected {expected_row_count}"


@demographic_categorical_summariser_test_suite.add_test
def test_frequencies_are_rounded_to_the_nearest_hes_rounding():
  input_df = long_format_df_fixture()
  frequency_rounding = HES_FREQUENCY_ROUNDING
  
  result_df = demographic_categorical_summariser(input_df)

  result_df = result_df.withColumn("REMAINDER", F.col("WEIGHT") % frequency_rounding)
  assert result_df.first() is not None  # Some rows

  # No rows with non-zero remainder
  assert result_df.filter("REMAINDER != 0.0").first() is None, f"Frequencies were not rounded to the nearest {frequency_rounding}"
  
  
@demographic_categorical_summariser_test_suite.add_test
def test_values_are_excluded_with_frequency_below_hes_minimum():
  input_df = long_format_df_fixture()
  min_frequency = HES_MIN_FREQUENCY
  
  result_df = demographic_categorical_summariser(input_df)
  assert result_df.first() is not None  # Some rows

  # No rows with weight below threshold
  assert result_df.filter(F.col("WEIGHT") < min_frequency).first() is None, f"Found frequencies below minimum {min_frequency}"
    
    
@demographic_categorical_summariser_test_suite.add_test
def test_values_are_excluded_with_frequency_below_threshold_within_rounding():
  # Need to make sure frequencies aren't rounded up so that they pass the threshold!
  input_df = long_format_df_fixture()
  
  result_df = demographic_categorical_summariser(input_df)
  expected_exclusions = (
    F
    .when(F.col("FIELD_NAME") == "field1", F.col("VALUE_STRING_ARRAY") == F.expr("array('x', 'z')"))
    .when(F.col("FIELD_NAME") == "field2", F.col("VALUE_STRING_ARRAY") == F.expr("array('x', null)"))
    .otherwise(False)
  )

  assert result_df.filter(expected_exclusions).first() is None, "Found values that should have been suppressed!"
  
  expected_row_count = 4
  actual_row_count = result_df.count() 
  assert actual_row_count == expected_row_count, f"Found {actual_row_count} rows but expected {expected_row_count}"

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x', 'y')")), 10.)
    .when((F.col("FIELD_NAME") == "field1") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x')")),      5.)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('x', 'y')")), 15.)
    .when((F.col("FIELD_NAME") == "field2") & (F.col("VALUE_STRING_ARRAY") == F.expr("array('y')")),      10.)
  )

  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_frequency_col)
  
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weights did not match expectations"
  

demographic_categorical_summariser_test_suite.run()

# COMMAND ----------

demographic_date_summariser_test_suite = FunctionTestSuite()


@demographic_date_summariser_test_suite.add_test
def test_correct_incidence_for_demographic_date_fields():
  input_df = long_format_df_fixture()
  input_df = input_df.unionByName(input_df.withColumn(HES_PATIENT_KEY, F.concat_ws("_", F.col(HES_PATIENT_KEY), F.lit("clone"))))  # Double up to avoid suppression without changing weights
  
  result_df = demographic_date_summariser(input_df)
  
  expected_row_count = 6  
  actual_row_count = result_df.filter(F.col("VALUE_TYPE") == "DEMOGRAPHIC_DATE").count()
  assert actual_row_count == expected_row_count, f"Found {actual_row_count} rows but expected {expected_row_count}"
  
  expected_incidence_col = (
    F
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == "2020-01-01"), 0.75)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == "2021-01-01"), 1.0)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == "2022-01-01"), 0.25)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == "2020-01-01"), 0.75)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == "2021-01-01"), 1.0)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == "2022-01-01"), 0.5)
    .cast("double")
  )
  
  result_df = result_df.withColumn("EXPECTED_VALUE_NUMERIC", expected_incidence_col)
  
  assert columns_equal(result_df, "VALUE_NUMERIC", "EXPECTED_VALUE_NUMERIC"), "Incidence for demographic date fields did not match expectations"
  
  
@demographic_date_summariser_test_suite.add_test
def test_frequencies_are_rounded_to_the_nearest_given_number():
  input_df = long_format_df_fixture()
  frequency_rounding = HES_FREQUENCY_ROUNDING
  
  result_df = demographic_date_summariser(input_df)

  result_df = result_df.withColumn("REMAINDER", F.col("WEIGHT") % frequency_rounding)
  assert result_df.first() is not None  # Some rows

  # No rows with non-zero remainder
  assert result_df.filter("REMAINDER != 0.0").first() is None, f"Frequencies were not rounded to the nearest {frequency_rounding}"
  
  
@demographic_date_summariser_test_suite.add_test
def test_values_are_excluded_with_frequency_below_threshold():
  input_df = long_format_df_fixture()
  min_frequency = HES_MIN_FREQUENCY
  
  result_df = demographic_date_summariser(input_df)
  assert result_df.first() is not None  # Some rows

  # No rows with weight below threshold
  assert result_df.filter(F.col("WEIGHT") < min_frequency).first() is None, f"Found frequencies below minimum {min_frequency}"
    
    
@demographic_date_summariser_test_suite.add_test
def test_values_are_excluded_with_frequency_below_threshold_within_rounding():
  # Need to make sure frequencies aren't rounded up so that they pass the threshold!
  input_df = long_format_df_fixture()
  
  result_df = demographic_date_summariser(input_df)
  expected_exclusions = (
    F
    .when(F.col("FIELD_NAME") == "field3", F.col("VALUE_STRING") == "2022-01-01")
    .when(F.col("FIELD_NAME") == "field4", F.col("VALUE_STRING") == "2022-01-01")
    .otherwise(False)
  )

  assert result_df.filter(expected_exclusions).first() is None, "Found values that should have been suppressed!"
  
  expected_row_count = 4
  actual_row_count = result_df.count() 
  assert actual_row_count == expected_row_count, f"Found {actual_row_count} rows but expected {expected_row_count}"

  expected_frequency_col = (
    F
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == "2020-01-01"), 10.)
    .when((F.col("FIELD_NAME") == "field3") & (F.col("VALUE_STRING") == "2021-01-01"), 5.)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == "2020-01-01"), 15.)
    .when((F.col("FIELD_NAME") == "field4") & (F.col("VALUE_STRING") == "2021-01-01"), 10.)
  )

  result_df = result_df.withColumn("EXPECTED_WEIGHT", expected_frequency_col)
  
  assert columns_equal(result_df, "WEIGHT", "EXPECTED_WEIGHT"), "Weights did not match expectations"
  

demographic_date_summariser_test_suite.run()

# COMMAND ----------

