# Databricks notebook source
# MAGIC %md
# MAGIC # Artificial Data Generator: Init Schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# MAGIC %run ./notebooks/common/widget_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

if check_databricks():
  dbutils.widgets.removeAll()
  dbutils.widgets.text("db",            "iuod_artificial_data_generator",     "0.1 Database")
  dbutils.widgets.text("notebook_root", "iuod_artificial_data_generator/dev", "0.2 Notebook Root")
else:
  # Only make widgets in databricks
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

database_name = get_required_argument("db")
create_meta_notebook_path = "./schemas/create_meta_table"
open_data_metadata_uplift_path = "./schemas/uplifts/open_data_metadata_uplift"

notebook_params = [
  {
    "path": open_data_metadata_uplift_path, 
    "timeout_seconds": 0, 
    "arguments": {
      "database_name": database_name, 
      "table_name": "artificial_hes_meta",
    }
  },
  {
    "path": create_meta_notebook_path, 
    "timeout_seconds": 300, 
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

