# Databricks notebook source
# MAGIC %md
# MAGIC # Artificial HES Schema Coercion
# MAGIC Coerces the schema of existing `artificial_hes` tables to match the schema specification for HES tables.

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ../../notebooks/common/widget_utils

# COMMAND ----------

# MAGIC %run ../../notebooks/common/spark_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/common/table_helpers

# COMMAND ----------

# MAGIC %run ../../notebooks/common/coerce_schema

# COMMAND ----------

# MAGIC %run ../../notebooks/dataset_definitions/hes/hes_schemas

# COMMAND ----------

# MAGIC %run ../../notebooks/dataset_definitions/hes/hes_tables

# COMMAND ----------

from typing import Iterable
from datetime import datetime

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("database_name", "artificial_hes", "0.1 Database Name")

# COMMAND ----------

database_name = get_required_argument("database_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def backup_and_uplift_schema(
  database_name: str, 
  table_name: str, 
  target_schema: T.StructType,
  partitionBy: Iterable[str]=[],
) -> bool:
  backup_version = datetime.now().strftime("%Y%m%d%H%M")
  backup_table_name = f"{table_name}_backup_{backup_version}"
  
  current_df = spark.table(f"{database_name}.{table_name}")
  
  if not datatypes_equal(current_df.schema, target_schema):
    print("Modifying schema to target")

    # Backup the current table
    create_table(
      spark,
      current_df,
      database_name,
      backup_table_name,
      format="delta",
      mode="overwrite",
      partitionBy=partitionBy,
    )

    # Drop the current table which has now been backed-up
    drop_table(spark, database_name, table_name)

    # Apply the schema changes
    uplifted_df = coerce_schema(
      spark.table(f"{database_name}.{backup_table_name}"),
      target_schema
    )

    # Write the new table
    create_table(
      spark,
      uplifted_df,
      database_name, 
      table_name,
      format="delta",
      mode="overwrite",
      partitionBy=partitionBy,
    )
    
    # Remove the backup
    drop_table(spark, database_name, backup_table_name)
  
  else:
    print("Current schema already equal to target schema!")

  target_df = spark.table(f"{database_name}.{table_name}")

  assert datatypes_equal(target_df.schema, target_schema)
  
  return True


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

for dataset_name, dataset_tables in HES_TABLES.items():
  target_schema = get_hes_schema(dataset_name)
  
  for hes_table in dataset_tables:    
    artificial_table_name = f"artificial_{hes_table.name}"
    
    if table_exists(spark, database_name, artificial_table_name):
      print(f"Uplifting schema for table {database_name}.{artificial_table_name}")

      backup_and_uplift_schema(
        database_name,
        artificial_table_name, 
        target_schema
      )
    else:
      continue

# COMMAND ----------

