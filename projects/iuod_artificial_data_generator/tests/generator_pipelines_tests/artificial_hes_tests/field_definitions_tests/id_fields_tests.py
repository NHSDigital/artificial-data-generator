# Databricks notebook source
# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %run ../../../../notebooks/dependencies/spark_rstr

# COMMAND ----------

# MAGIC %run ../../../../notebooks/generator_pipelines/artificial_hes/field_definitions/id_fields

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

rstr = SparkRstr()

# COMMAND ----------

# ID fields:

APC_ID_FIELDS = [
  "SUSRECID",
  "PSEUDO_HESID",
  "AEKEY",
  "EPIKEY",
]

OP_ID_FIELDS = [
  "PSEUDO_HESID",
  "ATTENDID",
  "ATTENDKEY",
  "PREFERER",
]

AE_ID_FIELDS = [
  "AEKEY",
  "PSEUDO_HESID",
  "EPIKEY"
]

# COMMAND ----------

# Patterns: 

ID_FIELD_PATTERNS = dict(
  PSEUDO_HESID = r"TEST[0-9a-zA-Z]{28}",  # 32an - first 4 chars = TEST to ensure no overlap with real IDs
  SUSRECID     = r"\d{14}",
  AEKEY        = r"\d{12}",               # Changes to r"\d{20}" in 2021/22
  EPIKEY       = r"\d{12}",               # Changes to r"\d{20}" in 2021/22
  ATTENDNO     = r"[0-9a-zA-Z]{12}",
  ATTENDKEY    = r"\d{12}",               # Changes to r"\d{20}" in 2021/22
  ATTENDID     = r"[0-9a-zA-Z]{12}",
  PREFERER     = r"[0-9a-zA-Z]{16}",      # What about nulls (&) / invalids (99)
)

# COMMAND ----------

test_get_id_fields = FunctionTestSuite()


@test_get_id_fields.add_test
def test_creates_hes_ae_columns():
  # Inputs
  df = spark.range(15)
  
  # Execute function under test
  id_field_cols = get_id_fields("hes_ae")
  for field_name in id_field_cols:
    df = df.withColumn(field_name, id_field_cols[field_name])
  
  for field in AE_ID_FIELDS:
    # Define expectations
    extracted_field_name = f'{field}_TEST'
    extracted_field = F.regexp_extract(field, ID_FIELD_PATTERNS[field], 0)
    df = df.withColumn(extracted_field_name, extracted_field)
    
    # Check against expectation
    assert columns_equal(df, field, extracted_field_name)

  
@test_get_id_fields.add_test
def test_creates_hes_apc_columns():
  # Inputs
  df = spark.range(15)
  
  # Execute function under test
  id_field_cols = get_id_fields("hes_apc")
  for field_name in id_field_cols:
    df = df.withColumn(field_name, id_field_cols[field_name])
    
  for field in APC_ID_FIELDS:
    # Define expectations
    extracted_field_name = f'{field}_TEST'
    extracted_field = F.regexp_extract(field, ID_FIELD_PATTERNS[field], 0)
    df = df.withColumn(extracted_field_name, extracted_field)
    
    # Check against expectation
    assert columns_equal(df, field, extracted_field_name)
  
  
@test_get_id_fields.add_test
def test_creates_hes_op_columns():
  # Inputs
  df = spark.range(15)
  
  # Execute function under test
  id_field_cols = get_id_fields("hes_op")
  for field_name in id_field_cols:
    df = df.withColumn(field_name, id_field_cols[field_name])
    
  for field in OP_ID_FIELDS:
   # Define expectations
    extracted_field_name = f'{field}_TEST'
    extracted_field = F.regexp_extract(field, ID_FIELD_PATTERNS[field], 0)
    df = df.withColumn(extracted_field_name, extracted_field)
    
    # Check against expectation
    assert columns_equal(df, field, extracted_field_name)

  
test_get_id_fields.run()