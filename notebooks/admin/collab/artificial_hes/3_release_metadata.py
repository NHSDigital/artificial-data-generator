# Databricks notebook source
# MAGIC %md # Release hes metadata
# MAGIC Run the `cp_run_notebooks_iuod_artificial_data_admin` to move HES metadata into the `iuod_artificial_data_generator` database.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../job_utils

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

USERNAME = ""
assert USERNAME, "Please provide a username for getting the API token"

job_params = {
  "safety_dt": "",
  "source_database_name": "artificial_hes_meta",
  "table_name": "artificial_hes_meta",
  "reviewer_name": "",
  "reviewer_email": "",
  "reviewed_date": "",
  "approver_name": "",
  "approver_email": "",
  "approved_date": "",
}

for param_name, param_value in job_params.items():
  assert param_value, f"Please provide a value for parameter '{job_params}'"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

token = dbutils.notebook.run(f"/Users/{USERNAME}/token", 60)
configure_databricks_cli(token)

# COMMAND ----------

run_job_async("cp_run_notebooks_iuod_artificial_data_admin", **job_params, sleep_seconds=30, max_retries=20)

# COMMAND ----------

