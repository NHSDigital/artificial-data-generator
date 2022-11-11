# Databricks notebook source
# MAGIC %md
# MAGIC # Artificial HES Meta: Init Schemas

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

if check_databricks():
  dbutils.widgets.removeAll()
  dbutils.widgets.text("notebook_root",     "artificial_hes_meta/dev", "0.1 Notebook Root")
  dbutils.widgets.text("db",                "artificial_hes_meta",     "0.2 Project Database")
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

database_name = get_required_argument("db")
adg_schemas_path = cp_project_params["adg_project_path"] / "schemas"
open_data_metadata_uplift_path =  str(adg_schemas_path / "uplifts" / "open_data_metadata_uplift")

notebook_params = [
  {
    "path": open_data_metadata_uplift_path, 
    "timeout_seconds": 0, 
    "arguments": {
      "database_name": database_name, 
      "table_name": "artificial_hes_meta",
    }
  },
]

# COMMAND ----------

for params in notebook_params:
  dbutils.notebook.run(**params)

# COMMAND ----------

  