# Databricks notebook source
# MAGIC %md # Generate artificial HES data
# MAGIC Run the`cp_run_notebooks_iuod_artificial_data_generator` job for the `artificial_hes` data asset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../job_utils

# COMMAND ----------

import json
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

USERNAME = ""
assert USERNAME, "Please provide a username for getting the API token"

parameters_json = json.dumps(dict(n_patients=1000000))
artificial_dataset = "artificial_hes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

token = dbutils.notebook.run(f"/Users/{USERNAME}/token", 60)
configure_databricks_cli(token)

# COMMAND ----------

run_job_async(
  "cp_run_notebooks_iuod_artificial_data_generator", 
  artificial_dataset=artificial_dataset,
  parameters_json=parameters_json, 
  sleep_seconds=0, 
  max_retries=1
)