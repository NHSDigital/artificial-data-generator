# Databricks notebook source
# MAGIC %run ../imports

# COMMAND ----------

# MAGIC %run ../../../../../notebooks/generator_pipelines/artificial_hes/field_definitions/derivations/age_fields

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def create_test_date_df():  
  df = spark.createDataFrame([
    (0, None,         "2021-12-31"),
    (1, "2021-12-31", "2021-12-31"), 
    (2, "2021-12-31", "2021-12-26"), 
    (3, "2021-12-31", "2021-12-04"), 
    (4, "2021-12-31", "2021-10-08"),
    (5, "2021-12-31", "2021-07-05"), 
    (6, "2021-12-31", "2021-05-04"), 
    (7, "2021-12-31", "2021-01-01"),
    (8, "2021-12-31", "1900-12-21"),
    (9, "2021-12-31", "2001-12-31"),
    ], ["id", "ADMIDATE", "DOB"]
  )
 
  return df


# COMMAND ----------

test_get_fractional_age_field = FunctionTestSuite()


@test_get_fractional_age_field.add_test
def test_creates_column_with_expected_values():
  # Inputs
  df = create_test_date_df()
  
  # Define expectations
  expected_fractional_age = (
    F.when(F.col("id") == 0, None)
    .when(F.col("id") == 1,   0.002)
    .when(F.col("id") == 2,   0.010)
    .when(F.col("id") == 3,   0.048)
    .when(F.col("id") == 4,   0.167)
    .when(F.col("id") == 5,   0.375)
    .when(F.col("id") == 6,   0.625)
    .when(F.col("id") == 7,   0.875)
    .when(F.col("id") == 8,     120)
    .when(F.col("id") == 9,    7305/365)
  )
  df = df.withColumn("EXPECTED_FRAC_AGE", expected_fractional_age)
  
  # Execute function under test
  fractional_age_calc = get_fractional_age_field("ADMIDATE", "DOB")
  df = df.withColumn("FRAC_AGE_CALC", fractional_age_calc)
  
  # Check against expectation
  assert columns_equal(df, "FRAC_AGE_CALC", "EXPECTED_FRAC_AGE")
  
  
test_get_fractional_age_field.run()

# COMMAND ----------

test_get_categorized_age_field = FunctionTestSuite()


@test_get_categorized_age_field.add_test
def test_creates_column_with_expected_values():
  # Inputs
  df = create_test_date_df()
  
  # Define expectations
  expected_categorized_age = (
    F.when(F.col("id") == 0, None)
    .when(F.col("id") == 1,   7001)
    .when(F.col("id") == 2,   7002)
    .when(F.col("id") == 3,   7003)
    .when(F.col("id") == 4,   7004)
    .when(F.col("id") == 5,   7005)
    .when(F.col("id") == 6,   7006)
    .when(F.col("id") == 7,   7007)
    .when(F.col("id") == 8,    120)
    .when(F.col("id") == 9,     20)
  )
  df = df.withColumn("EXPECTED_CAT_AGE", expected_categorized_age)
  
  # Execute function under test
  categorized_age_calc = get_categorized_age_field("ADMIDATE", "DOB")
  df = df.withColumn("CAT_AGE_CALC", categorized_age_calc)
  
  # Check against expectation
  assert columns_equal(df, "CAT_AGE_CALC", "EXPECTED_CAT_AGE")


test_get_categorized_age_field.run()

# COMMAND ----------

test_get_fractional_from_categorized_age_field = FunctionTestSuite()


@test_get_fractional_from_categorized_age_field.add_test
def test_creates_column_with_expected_values():
  # Inputs
  schema = "id: int, APPTAGE: int"
  df = spark.createDataFrame(
    [
      (0, None),
      (1, 7001),
      (2, 7002),
      (3, 7003),
      (4, 7004),
      (5, 7005),
      (6, 7006),
      (7, 7007),
      (8,  120),
      (9,   38)
    ],
    schema
  )
  
  # Define expectations
  expected_fractional_age = (
    F.when(F.col("id") == 0,   None)
    .when(F.col("id") == 1,   0.002)
    .when(F.col("id") == 2,   0.010)
    .when(F.col("id") == 3,   0.048)
    .when(F.col("id") == 4,   0.167)
    .when(F.col("id") == 5,   0.375)
    .when(F.col("id") == 6,   0.625)
    .when(F.col("id") == 7,   0.875)
    .when(F.col("id") == 8,     120)
    .when(F.col("id") == 9,      38)
  )
  df = df.withColumn("EXPECTED_APPTAGE_CALC", expected_fractional_age)
  
  # Execute function under test
  fractional_age = get_fractional_from_categorized_age_field("APPTAGE")
  df = df.withColumn("APPTAGE_CALC", fractional_age)
  
  # Check against expectation
  assert columns_equal(df, "APPTAGE_CALC", "EXPECTED_APPTAGE_CALC"), df.show()
  
  
test_get_fractional_from_categorized_age_field.run()

# COMMAND ----------

