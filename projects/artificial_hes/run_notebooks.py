# Databricks notebook source
# MAGIC %md ## Artificial HES: Run Notebooks
# MAGIC 
# MAGIC This notebook copies tables containing artificial HES data from `iuod_artificial_data_generator` into a target database (by default this is the CP project database, namely `artificial_hes`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ./notebooks/common/widget_utils

# COMMAND ----------

# MAGIC %run ./notebooks/common/table_helpers

# COMMAND ----------

from pprint import pprint
from functools import reduce

from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

if check_databricks():
  dbutils.widgets.removeAll()
  dbutils.widgets.text("notebook_root",                  "artificial_hes/dev",             "0.1 Notebook Root")
  dbutils.widgets.text("db",                             "artificial_hes",                 "0.2 Project Database")
  dbutils.widgets.text("iuod_artificial_data_generator", "iuod_artificial_data_generator", "0.3 ADG Project Database")
  dbutils.widgets.text("included_tables",                "",                               "1.1 Tables to copy (comma separated)")
  dbutils.widgets.text("excluded_tables",                "",                               "1.2 Tables to exclude from copy (comma separated)")
else:
  # Only make widgets in databricks
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

source_database = get_required_argument("iuod_artificial_data_generator")
target_database = get_required_argument("db")

# COMMAND ----------

# Parse the inclusions / exclusions

included_tables = list(filter(lambda x: x, dbutils.widgets.get("included_tables").split(",")))
excluded_tables = list(filter(lambda x: x, dbutils.widgets.get("excluded_tables").split(",")))

tables_to_clone_df = (
  spark.sql(rf"SHOW TABLES IN {source_database}")
  .filter(F.col("tableName").rlike(r"artificial_hes_(apc|ae|op)_\d{4}"))
  .filter(~F.col("tableName").isin(excluded_tables))
)

if included_tables:
  tables_to_clone_df = tables_to_clone_df.filter(F.col("tableName").isin(included_tables))
  
tables_to_clone = [row.tableName for row in tables_to_clone_df.collect()]
  
print("The following tables will be cloned: ")
pprint(tables_to_clone)

print("\nThe following tables will be excluded: ")
pprint(excluded_tables)

# COMMAND ----------

# NOTE: delta history logic commented out. We do want this but we will need to figure out how to ensure
# table permissions work across different environments. Currently the artificial data table gets created
# and the owner is set to admin or data-managers (on ref), but this then breaks the CP jobs permissions
# because it no longer owns the table so can't call 'DESCRIBE HISTORY'. Ticket added to the backlog to 
# revisit this.

# # Record the history of delta tables for users - only table owners can see the history otherwise!
# history_cols = [
#   F.col("version").alias("VERSION"),
#   F.col("timestamp").alias("TIMESTAMP"),
# ]
# history_dfs = []

for table_name in tables_to_clone:
  print(f"Cloning table `{table_name}` from database `{source_database}` to database `{target_database}`")
  source_df = spark.table(f"{source_database}.{table_name}")
  create_table(spark, source_df, target_database, table_name, mode="overwrite", overwriteSchema="true")
  
#   history_dfs.append(
#     spark.sql(f"DESCRIBE HISTORY {target_database}.{table_name}")
#     .select(
#       F.lit(table_name).alias("TABLE_NAME"),
#       *history_cols
#     )
#   )
  
# history_df = reduce(DataFrame.union, history_dfs)
# create_table(spark, history_df, target_database, "delta_history", mode="overwrite", overwriteSchema="true")

# COMMAND ----------

