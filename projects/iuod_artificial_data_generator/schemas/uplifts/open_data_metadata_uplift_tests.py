# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %run ../../notebooks/common/table_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/scraper_stages/schemas/meta_schema

# COMMAND ----------

import json
from pyspark.sql.types import StructType

database_name = "artificial_hes_meta"
table_name = "open_data_metadata_uplift_tests__artificial_hes_meta"
backup_table_name = f"{table_name}_v1_schema_backup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup tables

# COMMAND ----------

v1_meta_schema_json = '{"type": "struct", "fields": [{"name": "TABLE_NAME", "type": "string", "nullable": true, "metadata": {}}, {"name": "FIELD_NAME", "type": "string", "nullable": true, "metadata": {}}, {"name": "METADATA", "type": {"type": "struct", "fields": [{"name": "VALUE_TYPE", "type": "string", "nullable": true, "metadata": {}}, {"name": "FIELD_TYPE", "type": "string", "nullable": true, "metadata": {}}, {"name": "SUMMARY", "type": {"type": "struct", "fields": [{"name": "VALUE_TYPE", "type": "string", "nullable": true, "metadata": {}}, {"name": "CATEGORICAL", "type": {"type": "struct", "fields": [{"name": "VALUE_TYPE", "type": "string", "nullable": true, "metadata": {}}, {"name": "FLAT", "type": {"type": "struct", "fields": [{"name": "VALUE", "type": "string", "nullable": true, "metadata": {}}, {"name": "WEIGHT", "type": "double", "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}, {"name": "NESTED", "type": {"type": "struct", "fields": [{"name": "VALUE", "type": {"type": "array", "elementType": {"type": "struct", "fields": [{"name": "VALUE", "type": "string", "nullable": true, "metadata": {}}, {"name": "WEIGHT", "type": "double", "nullable": true, "metadata": {}}]}, "containsNull": true}, "nullable": true, "metadata": {}}, {"name": "WEIGHT", "type": "double", "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}, {"name": "DISCRETE", "type": {"type": "struct", "fields": [{"name": "VALUE", "type": "integer", "nullable": true, "metadata": {}}, {"name": "WEIGHT", "type": "double", "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}, {"name": "CONTINUOUS", "type": {"type": "struct", "fields": [{"name": "PERCENTILE", "type": "double", "nullable": true, "metadata": {}}, {"name": "VALUE", "type": "double", "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}, {"name": "DATE", "type": {"type": "struct", "fields": [{"name": "VALUE", "type": "date", "nullable": true, "metadata": {}}, {"name": "WEIGHT", "type": "double", "nullable": true, "metadata": {}}, {"name": "FORMAT", "type": "string", "nullable": true, "metadata": {}}, {"name": "NOISE", "type": {"type": "array", "elementType": {"type": "struct", "fields": [{"name": "REPLACEMENT", "type": "string", "nullable": true, "metadata": {}}, {"name": "WEIGHT", "type": "double", "nullable": true, "metadata": {}}]}, "containsNull": true}, "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}, {"name": "ID", "type": "string", "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}, {"name": "RELATIONSHIP", "type": {"type": "struct", "fields": [{"name": "LINKED_TABLE_NAME", "type": "string", "nullable": true, "metadata": {}}, {"name": "LINKED_FIELD_NAME", "type": "string", "nullable": true, "metadata": {}}, {"name": "MAPPING", "type": "long", "nullable": true, "metadata": {}}, {"name": "FREQUENCY", "type": "double", "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}]}, "nullable": true, "metadata": {}}]}'

v1_meta_schema = StructType.fromJson(json.loads(v1_meta_schema_json))

print(f"Creating table to test uplift: {database_name}.{table_name}")
create_table_from_schema(spark, database_name, table_name, v1_meta_schema)

# COMMAND ----------

# MAGIC %md # Run tests

# COMMAND ----------

# Try uplift twice: once represents fresh database, twice is to check that running it again doesn't break anything!
for i in range(2):
  # Uplift
  result = dbutils.notebook.run("./open_data_metadata_uplift", 0, {"database_name": database_name, "table_name": table_name})
  print(f"Uplift finished with response: {result}")
  
  # Test for backup
  assert table_exists(spark, database_name, backup_table_name), "Backup table doesn't exist"
  assert datatypes_equal(spark.table(f"{database_name}.{backup_table_name}").schema, v1_meta_schema), "Backup table has incorrect schema"

  # Test for uplifted
  uplift_meta_schema = get_meta_schema()
  assert table_exists(spark, database_name, table_name), "Uplift table doesn't exist"
  assert datatypes_equal(spark.table(f"{database_name}.{table_name}").schema, uplift_meta_schema), "Uplifted table has incorrect schema"


# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teardown tables

# COMMAND ----------

print("Cleaning up tables")
drop_table(spark, database_name, backup_table_name)
drop_table(spark, database_name, table_name)