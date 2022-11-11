# Databricks notebook source
# MAGIC %md
# MAGIC ## Admin Driver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ../common/table_helpers

# COMMAND ----------

# MAGIC %run ../common/widget_utils

# COMMAND ----------

import datetime
from datetime import datetime
import json
from typing import Iterable, Dict
from pyspark.sql import DataFrame, functions as F, Column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

if check_databricks():
  dbutils.widgets.removeAll()
  dbutils.widgets.text("safety_dt", "", "0 Safety Timestamp (UTC)")
  dbutils.widgets.text("config_json", "", "1 Config JSON")
else:
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get_release_data(config: Dict[str, str]) -> DataFrame:
  reviewed_date = datetime.strptime(config["reviewed_date"], "%Y-%m-%d")
  approved_date = datetime.strptime(config["approved_date"], "%Y-%m-%d")
  release_cols = [
    F.current_timestamp()                .alias("RELEASE_TIMESTAMP_UTC"),
    F.lit(config["source_database_name"]).alias("SOURCE_DATABASE_NAME"),
    F.lit(config["target_database_name"]).alias("TARGET_DATABASE_NAME"),
    F.lit(config["table_name"])          .alias("TABLE_NAME"),
    F.lit(config["reviewer_name"])       .alias("REVIEWER_NAME"),
    F.lit(config["reviewer_email"])      .alias("REVIEWER_EMAIL"),
    F.lit(reviewed_date).cast("date")    .alias("REVIEWED_DATE"),
    F.lit(config["approver_name"])       .alias("APPROVER_NAME"),
    F.lit(config["approver_email"])      .alias("APPROVER_EMAIL"),
    F.lit(approved_date).cast("date")    .alias("APPROVED_DATE"),
  ]

  return spark.range(1).select(*release_cols)

# COMMAND ----------

def check_safe() -> bool:
  safety_window_seconds = 600
  current_dt = datetime.now()
  safety_dt = datetime.strptime(get_required_argument("safety_dt"), "%Y-%m-%d %H:%M")
  safe = abs(current_dt - safety_dt).total_seconds() < safety_window_seconds
  return safe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

config = json.loads(get_required_argument("config_json"))
source_table_path = f"{config['source_database_name']}.{config['table_name']}"
target_table_path = f"{config['target_database_name']}.{config['table_name']}"

# COMMAND ----------

notebook_response = {}

if check_safe():
  print("Metadata marked as safe")

  print(f"Creating release log entry in table '{config['releases_database_name']}.releases'")
  
  release_df = get_release_data(config)  # Must be populated before release
  create_table(spark, release_df, config['releases_database_name'], "releases", format="delta", mode="append")

  print(f"Publishing from '{source_table_path}' to '{target_table_path}'")

  meta_df = spark.table(source_table_path)

  # Insert the rows into the target table
  # Table should already have been created by the owner)
  insert_into(spark, meta_df, *target_table_path.split("."), overwrite=True)
  spark.sql(f"OPTIMIZE {target_table_path}")
  
  print("Done!")

  notebook_response["status"] = "success"
  notebook_response["message"] = ""
else:
  notebook_response["status"] = "failed"
  notebook_response["message"] = "Failed safety check on user-entered timestamp"

# COMMAND ----------

dbutils.notebook.exit(json.dumps(notebook_response))

# COMMAND ----------

