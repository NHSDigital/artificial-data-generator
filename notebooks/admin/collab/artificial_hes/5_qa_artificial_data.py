# Databricks notebook source
# TODO: expand the notebook below with checks implemented in Ref

# COMMAND ----------

# MAGIC %run /Users/admin/releases/code-promotion/iuod_artificial_data_generator/4332+20220930151507.gite6c853018/notebooks/dataset_definitions/hes/hes_schemas

# COMMAND ----------

# MAGIC %run /Users/admin/releases/code-promotion/iuod_artificial_data_generator/4332+20220930151507.gite6c853018/notebooks/common/spark_helpers

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T


database_name = "artificial_hes"
table_name_pattern = r"artificial_hes_(op|apc|ae)_\d{4}"

# COMMAND ----------

tables = (
  spark.sql(f"show tables from {database_name}")
  .filter(F.col("tableName").rlike(table_name_pattern))
  .select("tableName")
  .collect()
)

for table in tables:
  artificial_df = spark.table(f"{database_name}.{table.tableName}")
  
  # Check non-zero rows
  row_count = artificial_df.count()
  assert row_count > 100, "Found fewer rows than expected"
  print(table.tableName, f"{row_count:,}")
  
  # Compare schemas
  if "ae" in table.tableName:
    hes_schema = get_hes_ae_schema()
  elif "op" in table.tableName:
    hes_schema = get_hes_op_schema()
  elif "apc" in table.tableName:
    hes_schema = get_hes_apc_schema()
  else:
    assert False, f"Table {table.table_name} does not belong to HES dataset (apc, op, ae)"
    
  # Sort for comparison between fields
  hes_schema = T.StructType(sorted(hes_schema.fields, key=lambda f: f.name))
  artificial_schema = artificial_df.select(*sorted(artificial_df.columns)).schema
  
  # TODO: 
  # Will the artificial_hes pipeline fail when it tries to update tha AE tables? 
  # Do we need to explicitly drop columns via init_schemas?
  assert datatypes_equal(artificial_schema, hes_schema), "Schema not equal to expected schema"
  
#   if len(hes_schema) != len(artificial_schema):
#     # TODO: handle this properly?
#     print(
#       [f for f in artificial_schema.fields if f.name not in hes_schema.fieldNames()], 
#       [f for f in hes_schema.fields if f.name not in artificial_schema.fieldNames()]
#     )  
#   else:
    
# TODO: Check values?

# COMMAND ----------

