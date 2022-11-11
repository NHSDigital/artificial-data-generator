# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/common/table_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/dataset_definitions/hes/hes_schemas

# COMMAND ----------

# MAGIC %run ../../notebooks/dataset_definitions/hes/hes_tables

# COMMAND ----------

from pyspark.sql import functions as F, types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup tables

# COMMAND ----------

target_schema = get_hes_ae_schema()

# Before the uplift there are some extra fields in the hes ae tables
# We want to test that these are removed by the uplift
current_schema = T.StructType([
  *target_schema.fields,
  T.StructField("DIAG_3_CONCAT", T.StringType(), True),
  T.StructField("DIAG_4_CONCAT", T.StringType(), True),
  T.StructField("DIAG_COUNT", T.StringType(), True),
  T.StructField("OPERTN_3_CONCAT", T.StringType(), True),
  T.StructField("OPERTN_4_CONCAT", T.StringType(), True),
  T.StructField("OPERTN_COUNT", T.StringType(), True),
])

database_name = "alistair_jones5_101351"
table_name = "artificial_hes_ae_1415"

# Recreate the table 
if table_exists(spark, database_name, table_name):
  drop_table(spark, database_name, table_name)
  
create_table_from_schema(
  spark,
  database_name,
  table_name,
  current_schema,
  format="delta",
  mode="overwrite",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run tests

# COMMAND ----------

# Should be able to run twice without failure
for i in range(2):
  dbutils.notebook.run("./artificial_hes_schema_coercion", 0, {"database_name": database_name})
  
  actual_schema = spark.table(f"{database_name}.{table_name}").schema
  assert datatypes_equal(target_schema, actual_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Teardown tables

# COMMAND ----------

drop_table(spark, database_name, table_name)