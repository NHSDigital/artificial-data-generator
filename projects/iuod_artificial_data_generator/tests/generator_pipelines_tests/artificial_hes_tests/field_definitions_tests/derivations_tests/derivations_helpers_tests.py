# Databricks notebook source
# MAGIC %run ../imports

# COMMAND ----------

# MAGIC %run ../../../../../notebooks/generator_pipelines/artificial_hes/field_definitions/derivations/derivations_helpers

# COMMAND ----------

from typing import Iterable, List
from functools import reduce
from itertools import zip_longest
from pprint import pprint
from pyspark.sql import DataFrame, types as T

# COMMAND ----------

# NOTE: the tests here should be high level, just checking that the columns exist
# and have the correct datatypes. Detailed value checking should be left to the specific
# tests designed for the underlying functions!

# COMMAND ----------

# Create test data/objects

def get_input_dataframe(*additional_fields: Iterable[T.StructField]) -> DataFrame:
  input_schema = T.StructType([
    T.StructField("MYDOB", T.StringType(), True),
    *additional_fields
  ])
  input_df = spark.createDataFrame([], input_schema)
  return input_df


def get_apc_sequential_fields_schemas() -> List[T.StructField]:
  return [
    T.StructField("ELECDATE", T.DateType(), True), 
    T.StructField("ADMIDATE", T.DateType(), True), 
    T.StructField("EPISTART", T.DateType(), True), 
    T.StructField("EPIEND", T.DateType(), True), 
    T.StructField("DISDATE", T.DateType(), True),
  ]


def get_apc_diag_4_fields_schemas() -> List[T.StructField]:
  return [
    T.StructField(f"DIAG_4_{i:02}", T.StringType(), False) for i in range(1, 21)
  ]


def get_op_diag_4_fields_schemas() -> List[T.StructField]:
  return get_apc_diag_4_fields_schemas()[:12]


def get_opertn_4_fields_schemas() -> List[T.StructField]:
  return [
    T.StructField(f"OPERTN_4_{i:02}", T.StringType(), False) for i in range(1, 25)
  ]

def get_apc_derivations_expected_schema() -> T.StructType():
  num_apc_diag_fields = 20
  num_apc_opertn_fields = 24
  expected_fields = [
    T.StructField("MYDOB", T.StringType(), True),
    T.StructField("STARTAGE_CALC", T.DoubleType(), True),
    T.StructField("STARTAGE", T.IntegerType(), True),
    T.StructField(f"OPERTN_3_CONCAT", T.StringType(), False),
    T.StructField(f"OPERTN_4_CONCAT", T.StringType(), False),
    T.StructField(f"OPERTN_COUNT", T.IntegerType(), True),
    T.StructField(f"DIAG_3_CONCAT", T.StringType(), False),
    T.StructField(f"DIAG_4_CONCAT", T.StringType(), False),
    T.StructField(f"DIAG_COUNT", T.IntegerType(), True),
    *[T.StructField(f"DIAG_3_{i:02}", T.StringType(), False) for i in range(1, 21)],
    *[T.StructField(f"DIAG_4_{i:02}", T.StringType(), False) for i in range(1, 21)],
    *[T.StructField(f"OPERTN_3_{i:02}", T.StringType(), False) for i in range(1, num_apc_opertn_fields+1)],
    *[T.StructField(f"OPERTN_4_{i:02}", T.StringType(), False) for i in range(1, num_apc_opertn_fields+1)],
    *get_apc_sequential_fields_schemas()
  ]
  expected_schema = T.StructType(sorted(expected_fields, key=lambda f: f.name))  # Alphabetical
  return expected_schema


def get_op_derivations_expected_schema() -> List[T.StructField]:
  num_op_diag_fields = 12
  num_op_opertn_fields = 24
  expected_fields = [
    T.StructField("MYDOB", T.StringType(), True),
    T.StructField("APPTAGE_CALC", T.DoubleType(), True),
    T.StructField("APPTAGE", T.IntegerType(), True),
    T.StructField(f"OPERTN_3_CONCAT", T.StringType(), False),
    T.StructField(f"OPERTN_4_CONCAT", T.StringType(), False),
    T.StructField(f"OPERTN_COUNT", T.IntegerType(), True),
    T.StructField(f"DIAG_3_CONCAT", T.StringType(), False),
    T.StructField(f"DIAG_4_CONCAT", T.StringType(), False),
    T.StructField(f"DIAG_COUNT", T.IntegerType(), True),
    *[T.StructField(f"DIAG_3_{i:02}", T.StringType(), False) for i in range(1, num_op_diag_fields+1)],
    *[T.StructField(f"DIAG_4_{i:02}", T.StringType(), False) for i in range(1, num_op_diag_fields+1)],
    *[T.StructField(f"OPERTN_3_{i:02}", T.StringType(), False) for i in range(1, num_op_opertn_fields+1)],
    *[T.StructField(f"OPERTN_4_{i:02}", T.StringType(), False) for i in range(1, num_op_opertn_fields+1)],
  ]
  expected_schema = T.StructType(sorted(expected_fields, key=lambda f: f.name))  # Alphabetical
  return expected_schema


def get_ae_derivations_expected_schema() -> List[T.StructField]:
  expected_fields = [
    T.StructField("MYDOB", T.StringType(), True),
    T.StructField("ARRIVALAGE_CALC", T.DoubleType(), True),
    T.StructField("ARRIVALAGE", T.IntegerType(), True),
    *[T.StructField(f"DIAG2_{i:02}", T.StringType(), False) for i in range(1, 13)],
    *[T.StructField(f"TREAT2_{i:02}", T.StringType(), False) for i in range(1, 13)],
    *[T.StructField(f"DIAG3_{i:02}", T.StringType(), False) for i in range(1, 13)],
    *[T.StructField(f"TREAT3_{i:02}", T.StringType(), False) for i in range(1, 13)],
  ]
  expected_schema = T.StructType(sorted(expected_fields, key=lambda f: f.name))  # Alphabetical
  return expected_schema
  

# COMMAND ----------

def assert_schemas_equal(expected_schema: T.StructType, actual_schema: T.StructType) -> None:
  try:
    assert datatypes_equal(expected_schema, actual_schema), "Expected and actual schemas are not equal! "
  except AssertionError as e:
    fields_in_expected_only = set(expected_schema.fieldNames()).difference(actual_schema.fieldNames())
    
    if len(fields_in_expected_only):
      print(f"Extra fields found in expected schema: {fields_in_expected_only}")

    fields_in_actual_only = set(actual_schema.fieldNames()).difference(expected_schema.fieldNames())    
    
    if len(fields_in_actual_only):
      print(f"Extra fields found in actual schema: {fields_in_actual_only}")
    
    if not fields_in_actual_only and not fields_in_expected_only:
      unequal_fields = list(filter(lambda fields: fields[0] != fields[1], zip_longest(expected_schema.fields, actual_schema.fields)))
      print("\nUnequal fields:\n")
      pprint(unequal_fields)
      
    raise e
   

# COMMAND ----------

test_get_apc_derivations = FunctionTestSuite()


@test_get_apc_derivations.add_test
def test_derived_fields_schema():  
  # The schema we expect when we select the derived fields onto a dataframe
  expected_schema = get_apc_derivations_expected_schema()
  
  # Dataframe with field dependencies for derivations
  input_df = get_input_dataframe(
    *get_apc_sequential_fields_schemas(),
    *get_apc_diag_4_fields_schemas(),
    *get_opertn_4_fields_schemas(),
  )
  
  # Select the derived fields onto the input dataframe to get the schema
  derivations = get_derivations("hes_apc")
  derived_df = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df)
  actual_schema = derived_df.select(*sorted(derived_df.columns)).schema
  
  assert_schemas_equal(expected_schema, actual_schema)
  
test_get_apc_derivations.run()

# COMMAND ----------

test_get_op_derivations = FunctionTestSuite()

  
@test_get_op_derivations.add_test
def test_derived_fields_schema():  
  # The schema we expect when we select the derived fields onto a dataframe
  expected_schema = get_op_derivations_expected_schema()
  
  # Dataframe with field dependencies for derivations
  input_df = get_input_dataframe(
    T.StructField("APPTAGE", T.IntegerType(), True),
    *get_op_diag_4_fields_schemas(),
    *get_opertn_4_fields_schemas(),
  )
  
  # Select the derived fields onto the input dataframe to get the schema
  derivations = get_derivations("hes_op")
  derived_df = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df)
  actual_schema = derived_df.select(*sorted(derived_df.columns)).schema
  
  assert_schemas_equal(expected_schema, actual_schema)
  
  
test_get_op_derivations.run()

# COMMAND ----------

test_get_ae_derivations = FunctionTestSuite()

  
@test_get_ae_derivations.add_test
def test_derived_fields_schema():
  # The schema we expect when we select the derived fields onto a dataframe
  expected_schema = get_ae_derivations_expected_schema()
  
  # Dataframe with field dependencies for derivations
  input_df = get_input_dataframe(
    T.StructField("ARRIVALAGE", T.IntegerType(), True),
    *[T.StructField(f"DIAG3_{i:02}", T.StringType(), False) for i in range(1, 13)],
    *[T.StructField(f"TREAT3_{i:02}", T.StringType(), False) for i in range(1, 13)],
  )

  # Select the derived fields onto the input dataframe to get the schema
  derivations = get_derivations("hes_ae")
  derived_df = reduce(lambda df, derived_field: df.withColumn(*derived_field), derivations, input_df)
  actual_schema = derived_df.select(*sorted(derived_df.columns)).schema
  
  assert_schemas_equal(expected_schema, actual_schema)
  
  
test_get_ae_derivations.run()

# COMMAND ----------

