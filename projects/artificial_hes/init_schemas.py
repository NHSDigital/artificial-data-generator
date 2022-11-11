# Databricks notebook source
# MAGIC %md ## Artificial HES: Init Schemas
# MAGIC This is the init_schemas notebook for the artificial_hes Code Promotion project

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
  dbutils.widgets.text("notebook_root",                  "artificial_hes/dev",             "0.1 Notebook Root")
  dbutils.widgets.text("db",                             "artificial_hes",                 "0.2 Project Database")
  dbutils.widgets.text("iuod_artificial_data_generator", "iuod_artificial_data_generator", "0.3 ADG Project Database")
else:
  # Only make widgets in databricks
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main
# MAGIC Run schema notebooks

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
artificial_hes_schema_coercion_path =  str(adg_schemas_path / "uplifts" / "artificial_hes_schema_coercion")

notebook_params = [
  {
    "path": "./schemas/create_user_docs", 
    "timeout_seconds": 0, 
    "arguments": {"database_name": database_name}
  },
  {
    "path": artificial_hes_schema_coercion_path, 
    "timeout_seconds": 0, 
    "arguments": {"database_name": database_name}
  },
]

# COMMAND ----------

for params in notebook_params:
  dbutils.notebook.run(**params)