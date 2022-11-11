# Databricks notebook source
# MAGIC %md # Scrape HES metadata
# MAGIC Run the `cp_run_notebooks_artificial_hes_meta` job with user-entered config.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../job_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

USERNAME = ""
assert USERNAME, "Please provide a username for getting the API token"

token = dbutils.notebook.run(f"/Users/{USERNAME}/token", 60)
configure_databricks_cli(token)

# COMMAND ----------

run_job_async("cp_run_notebooks_artificial_hes_meta", sleep_seconds=0, max_retries=1)

# COMMAND ----------

