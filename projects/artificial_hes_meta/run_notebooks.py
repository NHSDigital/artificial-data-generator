# Databricks notebook source
# MAGIC %md
# MAGIC # Artificial HES Meta: Run Notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code Promotion Project Paths

# COMMAND ----------

# MAGIC %run ./notebooks/code_promotion_paths

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ./notebooks/common/widget_utils

# COMMAND ----------

import pprint
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

if check_databricks():
  dbutils.widgets.removeAll()
  dbutils.widgets.text("notebook_root",     "artificial_hes_meta/dev", "0.1 Notebook Root")
  dbutils.widgets.text("db",                "artificial_hes_meta",     "0.2 Project Database")
  dbutils.widgets.text("hes",               "hes",                     "0.3 HES Database")
else:
  # Only make widgets in databricks
  pass

# COMMAND ----------

# MAGIC %md ## Main

# COMMAND ----------

notebook_root = get_required_argument("notebook_root")
*_, project_name, project_version = notebook_root.split("/")

cp_project_params = {}
cp_project_params["project_name"] = project_name
cp_project_params["project_version"] = project_version 
cp_project_params["adg_project_path"] = get_adg_project_path(project_version)

# COMMAND ----------

print(f"Running with context:") 
pprint.pprint(cp_project_params)

# COMMAND ----------

params = {
  "path": str(cp_project_params["adg_project_path"] / "notebooks" / "scraper_pipelines" / "hes" / "driver"),
  "timeout_seconds": 0,
  "arguments": {
    "hes_database": get_required_argument("hes"),
    "meta_database": get_required_argument("db"),
  }
}

dbutils.notebook.run(**params)

# COMMAND ----------

