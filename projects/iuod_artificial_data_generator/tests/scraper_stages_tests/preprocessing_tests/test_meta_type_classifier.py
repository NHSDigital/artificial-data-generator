# Databricks notebook source
# MAGIC %run ../../test_helpers

# COMMAND ----------

# MAGIC %run ../../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run  ../../../notebooks/scraper_stages/preprocessing/meta_type_classifier

# COMMAND ----------

from pyspark.sql import (
  DataFrame, 
  Column, 
  functions as F, 
  types as T
)

# COMMAND ----------

test_suite = FunctionTestSuite()
# region classify_meta_types tests
@test_suite.add_test
def test_correctly_classifies_dates():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random dates within strings
  date_fmt = "yyyy-MM-dd"
  input_df = input_df.withColumn("DATE_FIELD", F.expr("date_add(to_date('2021-01-01'), rand() * 365)")).drop("id")
  
  classified_fields = classify_meta_types(input_df)
  assert classified_fields["DATE"] == ["DATE_FIELD"]
  assert not classified_fields["CONTINUOUS"]
  assert not classified_fields["CATEGORICAL"]
  
@test_suite.add_test
def test_correctly_classifies_dates_within_strings():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random dates within strings
  date_fmt = "yyyy-MM-dd"
  input_df = input_df.withColumn("DATE_FIELD", F.date_format(F.expr("date_add(to_date('2021-01-01'), rand() * 365)"), date_fmt)).drop("id")
  
  classified_fields = classify_meta_types(input_df)
  assert classified_fields["DATE"] == ["DATE_FIELD"]
  assert not classified_fields["CONTINUOUS"]
  assert not classified_fields["CATEGORICAL"]
  
@test_suite.add_test
def test_correctly_classifies_dates_with_different_formats():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random dates within strings
  date_fmts = ["yyyy-MM-dd", "yyyyMMdd", "dd/MM/yyyy", "dd/MM/yy"]
  for fmt in date_fmts:
    input_df = input_df.withColumn(f"DATE_FIELD_{fmt}", F.date_format(F.expr("date_add(to_date('2021-01-01'), rand() * 365)"), fmt)).drop("id")
  
  classified_fields = classify_meta_types(input_df)
  
  assert len(classified_fields["DATE"]) == len(date_fmts)
  for fmt in date_fmts:
    assert f"DATE_FIELD_{fmt}" in classified_fields["DATE"]
    
@test_suite.add_test
def test_correctly_classifies_nullable_dates():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random dates within strings
  date_fmt = "yyyy-MM-dd"
  input_df = input_df.withColumn("DATE_FIELD", F.expr("date_add(to_date('2021-01-01'), rand() * 365)")).drop("id")
  
  # Randomly set nulls
  null_frac = 0.1
  input_df = input_df.withColumn("DATE_FIELD", F.when(F.rand() > null_frac, F.col("DATE_FIELD")))
  
  classified_fields = classify_meta_types(input_df)
  assert classified_fields["DATE"] == ["DATE_FIELD"]
  assert not classified_fields["CONTINUOUS"]
  assert not classified_fields["CATEGORICAL"]
  
@test_suite.add_test
def test_correctly_classifies_continuous():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random continuous variables
  input_df = input_df.withColumn("CONTINUOUS_FIELD", F.rand()).drop("id")
  
  classified_fields = classify_meta_types(input_df)
  assert classified_fields["CONTINUOUS"] == ["CONTINUOUS_FIELD"]
  assert not classified_fields["DATE"]
  assert not classified_fields["CATEGORICAL"]
  
@test_suite.add_test
def test_correctly_classifies_continuous_within_strings():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random continuous variables within strings
  input_df = input_df.withColumn("CONTINUOUS_FIELD", F.rand().cast("string")).drop("id")
  
  classified_fields = classify_meta_types(input_df)
  assert classified_fields["CONTINUOUS"] == ["CONTINUOUS_FIELD"]
  assert not classified_fields["DATE"]
  assert not classified_fields["CATEGORICAL"]
  
@test_suite.add_test
def test_correctly_classifies_nullable_continuous():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random continuous variables
  input_df = input_df.withColumn("CONTINUOUS_FIELD", F.rand()).drop("id")
  
  # Randomly set nulls
  null_frac = 0.1
  input_df = input_df.withColumn("CONTINUOUS_FIELD", F.when(F.rand() > null_frac, F.col("CONTINUOUS_FIELD")))
  
  classified_fields = classify_meta_types(input_df, sample_frac=1.0)
  
  assert classified_fields["CONTINUOUS"] == ["CONTINUOUS_FIELD"]
  assert not classified_fields["DATE"]
  assert not classified_fields["CATEGORICAL"]
  
@test_suite.add_test
def test_correctly_classifies_categorical_strings():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random cateogircal variables within strings
  input_df = input_df.withColumn("CATEGORICAL_FIELD", F.round(F.rand() * 10, 0).cast("string")).drop("id")
  
  classified_fields = classify_meta_types(input_df, sample_frac=1.0)
  assert classified_fields["CATEGORICAL"] == ["CATEGORICAL_FIELD"]
  assert not classified_fields["DATE"]
  assert not classified_fields["CONTINUOUS"]
  
@test_suite.add_test
def test_correctly_classifies_nullable_categorical_strings():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random cateogircal variables within strings
  input_df = input_df.withColumn("CATEGORICAL_FIELD", F.round(F.rand() * 10, 0).cast("string")).drop("id")
  
  # Randomly set nulls
  null_frac = 0.1
  input_df = input_df.withColumn("CATEGORICAL_FIELD", F.when(F.rand() > null_frac, F.col("CATEGORICAL_FIELD")))
  
  classified_fields = classify_meta_types(input_df, sample_frac=1.0)
  assert classified_fields["CATEGORICAL"] == ["CATEGORICAL_FIELD"]
  assert not classified_fields["DATE"]
  assert not classified_fields["CONTINUOUS"]
  
@test_suite.add_test
def test_correctly_classifies_categorical_numbers_based_on_threshold():
  nrows = 1000
  input_df = spark.range(nrows)
  
  # Column of random continuous variables within strings
  threshold_distinct_count = 100
  input_df = input_df.withColumn("CATEGORICAL_FIELD", F.floor(F.rand() * 95)).drop("id")
  input_df = input_df.withColumn("CONTINUOUS_FIELD", F.floor(F.rand() * 105)).drop("id")
  
  classified_fields = classify_meta_types(input_df, threshold_distinct_count=threshold_distinct_count, sample_frac=1.0)
  assert classified_fields["CATEGORICAL"] == ["CATEGORICAL_FIELD"]
  assert classified_fields["CONTINUOUS"] == ["CONTINUOUS_FIELD"]
  assert not classified_fields["DATE"]
  
#endregion
test_suite.run()

# COMMAND ----------

