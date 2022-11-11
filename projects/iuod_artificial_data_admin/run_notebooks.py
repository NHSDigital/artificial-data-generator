# Databricks notebook source
# MAGIC %md
# MAGIC ## Run Notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code Promotion Version

# COMMAND ----------

# MAGIC %run ./notebooks/code_promotion_versions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ./notebooks/code_promotion_paths

# COMMAND ----------

# MAGIC %run ./notebooks/widget_utils

# COMMAND ----------

import json
from typing import Dict

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

if check_databricks():
  # TODO: update these!
  dbutils.widgets.removeAll()
  dbutils.widgets.text("notebook_root",                  "iuod_artificial_data_admin/dev", "0.1 Notebook Root")
  dbutils.widgets.text("db",                             "iuod_artificial_data_admin",     "0.2 Project Database")
  dbutils.widgets.text("iuod_artificial_data_generator", "iuod_artificial_data_generator", "0.3 ADG Project Database")
  dbutils.widgets.text("artificial_hes_meta",             "artificial_hes_meta",           "0.4 HES Meta Project Database")
  dbutils.widgets.text("safety_dt",                       "",                              "1.1 Safety Timestamp (UTC)")
  dbutils.widgets.text("source_database_name",            "",                              "2.1 Source Database Name")
  dbutils.widgets.text("table_name",                      "",                              "2.2 Source / Target Table Name")
  dbutils.widgets.text("reviewer_name",                   "",                              "3.1 Reviewer Name")
  dbutils.widgets.text("reviewer_email",                  "",                              "3.2 Reviewer Email")
  dbutils.widgets.text("reviewed_date",                   "",                              "3.3 Reviewed Date")
  dbutils.widgets.text("approver_name",                   "",                              "3.4 Approver Name")
  dbutils.widgets.text("approver_email",                  "",                              "3.5 Approver Email")
  dbutils.widgets.text("approved_date",                   "",                              "3.6 Approved Date")
else:
  # Only make widgets in databricks
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get_adg_env(project_version: str) -> str:
  if project_version == "dev":
    return "dev"
  elif project_version == "staging":
    return "staging"
  else:
    return "release"  

# COMMAND ----------

def get_driver_arguments() -> Dict[str, str]:
  source_database_name = get_required_argument(get_required_argument("source_database_name"))  # Must be one of the databases in the project permissions scope
  return {
    "safety_dt": get_required_argument("safety_dt"),
    "config_json": json.dumps(dict(
      source_database_name   = source_database_name,
      target_database_name   = get_required_argument("iuod_artificial_data_generator"),
      table_name             = get_required_argument("table_name"),
      reviewer_name          = get_required_argument("reviewer_name"),
      reviewer_email         = get_required_argument("reviewer_email"),
      reviewed_date          = get_required_argument("reviewed_date"),
      approver_name          = get_required_argument("approver_name"),
      approver_email         = get_required_argument("approver_email"),
      approved_date          = get_required_argument("approved_date"),
      releases_database_name = get_required_argument("db"),
    ))
  }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

# TODO: wrap in try catch
driver_args = get_driver_arguments()  # All args must be provided to proceed!

# COMMAND ----------

notebook_response = {}

# COMMAND ----------

notebook_root = get_required_argument("notebook_root")
*_, project_name, project_version = notebook_root.split("/")

adg_env = get_adg_env(project_version)
adg_project_path = get_adg_project_path(adg_env, ADG_PROJECT_VERSION)

# COMMAND ----------

driver_params = {
  "path": str(adg_project_path / "notebooks" / "admin" / "driver"),
  "timeout_seconds": 0,
  "arguments": get_driver_arguments()
}
driver_response_json = dbutils.notebook.run(**driver_params)

notebook_response["status"] = "success"
notebook_response["message"] = ""
notebook_response["children"] = [{**driver_params, "response": json.loads(driver_response_json)}]

# COMMAND ----------

dbutils.notebook.exit(json.dumps(notebook_response))

# COMMAND ----------

