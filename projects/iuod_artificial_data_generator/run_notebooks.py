# Databricks notebook source
# MAGIC %md
# MAGIC # Artificial Data Generator: Run Notebooks
# MAGIC This notebook runs the data generator pipeline for a given artificial dataset. The artificial data will be written into the database specified by the `CP Project Database` argument. 
# MAGIC 
# MAGIC **Prerequisites:** The underlying driver will ingest the metadata that was scraped from the real data and signed off against the disclosure control checklist - the metadata must exist within a database that is readable by this code promotion project (aka _iuod_artificial_data_generator_). The database containing metadata is specified via the `CP Project Database` argument.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import dependencies

# COMMAND ----------

# MAGIC %run ./notebooks/common/widget_utils

# COMMAND ----------

from pprint import pprint
from pathlib import Path

GENERATOR_PIPELINES_ROOT = Path("./notebooks/generator_pipelines")
AVAILABLE_DRIVERS = [
  "artificial_hes"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create widgets

# COMMAND ----------

if check_databricks():
    dbutils.widgets.removeAll()
    dbutils.widgets.text("db",                 "iuod_artificial_data_generator",   "0.1 CP Project Database")
    dbutils.widgets.text("artificial_dataset", "",                                 "1.1 Artificial Dataset")
    dbutils.widgets.text("parameters_json",    "",                                 "1.2 Parameters JSON")
else:
  # Only make widgets in databricks
  pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driver execution
# MAGIC Trigger the driver notebook for the artificial dataset specified via the arguments

# COMMAND ----------

artificial_dataset = get_required_argument("artificial_dataset")

if artificial_dataset not in AVAILABLE_DRIVERS:
  message = f"Driver does not exist for dataset '{artificial_dataset}': please choose from {AVAILABLE_DRIVERS}"
  dbutils.notebook.exit(json.dumps({"status": "failed", "message": message}))
  
# Path to the notebook that generates data for the given dataset
driver_path = GENERATOR_PIPELINES_ROOT / artificial_dataset / "driver"

# Prepare driver parameters
database = get_required_argument("db")
driver_args = dict(
    meta_database       = database,
    artificial_database = database,
    parameters_json     = get_required_argument("parameters_json"),
)

print(f"Running driver {str(driver_path)} with arguments:")
pprint(driver_args)

driver_response = dbutils.notebook.run(str(driver_path), 0, arguments=driver_args)

dbutils.notebook.exit(driver_response)

# COMMAND ----------

