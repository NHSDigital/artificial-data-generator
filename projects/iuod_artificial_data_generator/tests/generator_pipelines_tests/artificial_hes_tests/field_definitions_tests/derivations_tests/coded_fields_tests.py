# Databricks notebook source
# MAGIC %run ../imports

# COMMAND ----------

# MAGIC %run ../../../../../notebooks/generator_pipelines/artificial_hes/field_definitions/derivations/coded_fields

# COMMAND ----------

import string
import random
from functools import reduce

# COMMAND ----------

test_get_opertn_34_fields = FunctionTestSuite()


@test_get_opertn_34_fields.add_test
def test_opertn_3_fields():
  # Input data with OPERTN_4_01='0123', OPERTN_4_02='1234', OPERTN_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(24)]], opertn_4_fields)
  
  # Apply derivations
  derivations = get_opertn_34_fields()
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()  # Apply derivations
  
  for i in range(24):
    expected_value = string.ascii_letters[i:i+3]
    actual_value = actual_row[f"OPERTN_3_{i+1:02}"]
    assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
    

@test_get_opertn_34_fields.add_test
def test_opertn_3_concat_field():
  # Input data with OPERTN_4_01='0123', OPERTN_4_02='1234', OPERTN_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(24)]], opertn_4_fields)
  
  # Apply derivations
  derivations = get_opertn_34_fields()
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()  # Apply derivations
  
  actual_value = actual_row["OPERTN_3_CONCAT"]
  expected_value = ",".join([string.ascii_letters[i:i+3] for i in range(24)])
  assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  
  
@test_get_opertn_34_fields.add_test
def test_opertn_4_concat_field():
  # Input data with OPERTN_4_01='0123', OPERTN_4_02='1234', OPERTN_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(24)]], opertn_4_fields)
  
  # Apply derivations
  derivations = get_opertn_34_fields()
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()  # Apply derivations
  
  actual_value = actual_row["OPERTN_4_CONCAT"]
  expected_value = ",".join([string.ascii_letters[i:i+4] for i in range(24)])
  assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"

  
@test_get_opertn_34_fields.add_test
def test_opertn_count_field():
  # Input data with OPERTN_4_01='0123', OPERTN_4_02='1234', OPERTN_4_03='2345' etc
  # Fields are knocked-out at random
  input_data = [
    [
      *[string.ascii_letters[i:i+4] for i in range(j)], 
      *[None for i in range(24-j)]
    ] for j in range(24)
  ]
  list(map(random.shuffle, input_data))  # Random knock-out across rows
  input_schema = T.StructType([T.StructField(name, T.StringType(), True) for name in opertn_4_fields])
  input_df = spark.createDataFrame(input_data, input_schema)
  
  # Apply derivations
  derivations = get_opertn_34_fields()
  result_df = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df)
  
  actual_rows = result_df.select("OPERTN_COUNT").orderBy("OPERTN_COUNT").collect()
  
  for i, actual_row in enumerate(actual_rows):
    expected_value = i if i > 0 else None
    actual_value = actual_row["OPERTN_COUNT"]
    assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  
  
test_get_opertn_34_fields.run()

# COMMAND ----------

test_get_diag_34_fields = FunctionTestSuite()


@test_get_diag_34_fields.add_test
def test_diag_3_fields():
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(20)]], diag_4_fields)
  
  # Apply derivations
  derivations = get_diag_34_fields()
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()
  
  for i in range(20):
    expected_value = string.ascii_letters[i:i+3]
    actual_value = actual_row[f"DIAG_3_{i+1:02}"]
    assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  
  
@test_get_diag_34_fields.add_test
def test_diag_3_fields_with_limit():
  limit = 12  # OP has 12 DIAG_3/DIAG_4 fields
  
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(limit)]], diag_4_fields[:limit])
  
  # Apply derivations
  derivations = get_diag_34_fields(limit=limit)
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()
  
  for i in range(limit):
    expected_value = string.ascii_letters[i:i+3]
    actual_value = actual_row[f"DIAG_3_{i+1:02}"]
    assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  

@test_get_diag_34_fields.add_test
def test_diag_3_concat_field():
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(20)]], diag_4_fields)
  
  # Apply derivations
  derivations = get_diag_34_fields()
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()
  
  actual_value = actual_row["DIAG_3_CONCAT"]
  expected_value = ",".join([string.ascii_letters[i:i+3] for i in range(20)])
  assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  
  
@test_get_diag_34_fields.add_test
def test_diag_3_concat_field_with_limit():
  limit = 12  # OP has 12 DIAG_3/DIAG_4 fields
  
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(limit)]], diag_4_fields[:limit])
  
  # Apply derivations
  derivations = get_diag_34_fields(limit=limit)
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()
  
  actual_value = actual_row["DIAG_3_CONCAT"]
  expected_value = ",".join([string.ascii_letters[i:i+3] for i in range(limit)])
  assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  
  
@test_get_diag_34_fields.add_test
def test_diag_4_concat_field():
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(20)]], diag_4_fields)
  
  # Apply derivations
  derivations = get_diag_34_fields()
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()
  
  actual_value = actual_row["DIAG_4_CONCAT"]
  expected_value = ",".join([string.ascii_letters[i:i+4] for i in range(20)])
  assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"


@test_get_diag_34_fields.add_test
def test_diag_4_concat_field_with_limit():
  limit = 12
  
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  input_df = spark.createDataFrame([[string.ascii_letters[i:i+4] for i in range(limit)]], diag_4_fields[:limit])
  
  # Apply derivations
  derivations = get_diag_34_fields(limit=limit)
  actual_row = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df).first()
  
  actual_value = actual_row["DIAG_4_CONCAT"]
  expected_value = ",".join([string.ascii_letters[i:i+4] for i in range(limit)])
  assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  
  
@test_get_diag_34_fields.add_test
def test_diag_count_field():
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  # Fields are knocked-out at random
  input_data = [
    [
      *[string.ascii_letters[i:i+4] for i in range(j)], 
      *[None for i in range(20-j)]
    ] for j in range(20)
  ]
  list(map(random.shuffle, input_data))  # Random knock-out across rows
  input_schema = T.StructType([T.StructField(name, T.StringType(), True) for name in diag_4_fields])
  input_df = spark.createDataFrame(input_data, input_schema)
  
  # Apply derivations
  derivations = get_diag_34_fields()
  actual_rows = (
    reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df)
    .select("DIAG_COUNT")
    .orderBy("DIAG_COUNT")
    .collect()
  )
  
  for i, actual_row in enumerate(actual_rows):
    expected_value = i if i > 0 else None
    actual_value = actual_row["DIAG_COUNT"]
    assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
  

@test_get_diag_34_fields.add_test
def test_diag_count_field_with_limit():
  limit = 12
  
  # Input data with DIAG_4_01='0123', DIAG_4_02='1234', DIAG_4_03='2345' etc
  # Fields are knocked-out at random
  input_data = [
    [
      *[string.ascii_letters[i:i+4] for i in range(j)], 
      *[None for i in range(limit-j)]
    ] for j in range(limit)
  ]
  list(map(random.shuffle, input_data))  # Random knock-out across rows
  input_schema = T.StructType([T.StructField(name, T.StringType(), True) for name in diag_4_fields[:limit]])
  input_df = spark.createDataFrame(input_data, input_schema)
  
  # Apply derivations
  derivations = get_diag_34_fields(limit=limit)
  result_df = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df)
  
  actual_rows = (
    result_df
    .select("DIAG_COUNT")
    .orderBy("DIAG_COUNT")
    .collect()
  )
  
  for i, actual_row in enumerate(actual_rows):
    expected_value = i if i > 0 else None
    actual_value = actual_row["DIAG_COUNT"]
    assert expected_value == actual_value, f"Expected value ({expected_value}) != actual value ({actual_value})"
    
    
test_get_diag_34_fields.run()

# COMMAND ----------

