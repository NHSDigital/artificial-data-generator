# Databricks notebook source
# MAGIC %md
# MAGIC # Create Metadata Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ../notebooks/common/widget_utils

# COMMAND ----------

# MAGIC %run ../notebooks/common/table_helpers

# COMMAND ----------

# MAGIC %run ../notebooks/scraper_stages/schemas/meta_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text(    "database_name",  "",      "1.1 Database Name")
dbutils.widgets.text(    "table_name",     "",      "1.2 Table Name")
dbutils.widgets.dropdown("replace_if_exists", "false", ["true", "false"], "2.1 Replace If Exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

database_name = get_required_argument("database_name")
table_name = get_required_argument("table_name")
replace_if_exists = dbutils.widgets.get("replace_if_exists") == "true"

# COMMAND ----------

if database_exists(spark, database_name, verbose=True):
  if table_exists(spark, database_name, table_name):
    if replace_if_exists:
      drop_table(spark, database_name, table_name)
    else:
      dbutils.notebook.exit({"status": "skipped", "message": "Table exists but replace_if_exists=False"})
  
  create_table_from_schema(
    spark,
    database_name, 
    table_name, 
    get_meta_schema(), 
    mode="overwrite",
    format="delta",
    partitionBy=["TABLE_NAME", "FIELD_NAME"]
  )

# COMMAND ----------

