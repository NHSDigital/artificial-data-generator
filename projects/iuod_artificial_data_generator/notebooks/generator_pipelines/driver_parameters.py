# Databricks notebook source
import json
from pprint import pprint

# Prevent spark from auto broadcasting the metadata
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ARTIFICIAL_TABLE_NAME_TEMPLATE = "artificial_{table_name}"

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("meta_database",       "iuod_artificial_data_generator", "0.1 Meta DB")
dbutils.widgets.text("artificial_database", "iuod_artificial_data_generator", "0.2 Artificial DB")
dbutils.widgets.text("parameters_json",     "",                               "1.1 Parameters JSON")

# COMMAND ----------

# Config
notebook_parameters = json.loads(get_required_argument("parameters_json"))
n_patients = notebook_parameters['n_patients']
artificial_database = get_required_argument("artificial_database")
meta_database = get_required_argument("meta_database")

print("Running with parameters: ")
pprint({
  "n_patients": n_patients,
  "artificial_database": artificial_database,
  "meta_database": meta_database,
})