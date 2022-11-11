# Databricks notebook source
# MAGIC %run ../../dataset_definitions/hes/hes_tables

# COMMAND ----------

# MAGIC %run ./coerce_hes_schema

# COMMAND ----------

# MAGIC %run ./field_definitions/derivations/derivations_helpers

# COMMAND ----------

# MAGIC %run ./field_definitions/id_fields

# COMMAND ----------

# MAGIC %run ./demographic_field_generators

# COMMAND ----------

# MAGIC %run ../driver_imports

# COMMAND ----------

import json
from pprint import pprint
from pyspark.sql import DataFrame, functions as F

# Prevent spark from auto broadcasting the metadata
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

ARTIFICIAL_TABLE_NAME_TEMPLATE = "artificial_{table_name}"

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("meta_database",       "iuod_artificial_data_generator", "0.1 Meta DB")
dbutils.widgets.text("artificial_database", "iuod_artificial_data_generator", "0.2 Artificial DB")
dbutils.widgets.text("parameters_json",     json.dumps({"n_patients": 100}),  "1.1 Parameters JSON")

# COMMAND ----------

# Config
meta_database = get_required_argument("meta_database")
artificial_database = get_required_argument("artificial_database")

notebook_parameters = json.loads(get_required_argument("parameters_json"))
n_patients = notebook_parameters['n_patients']

print("Running with parameters: ")
pprint({
  "n_patients": n_patients,
  "artificial_database": artificial_database,
  "meta_database": meta_database,
})

# COMMAND ----------

variable_fields = ["FIELD_NAME"]
index_fields = ["ARTIFICIAL_DEMOGRAPHIC_ID", "ARTIFICIAL_EPISODE_ID"]
  

def generate_artificial_hes_table(meta_df: DataFrame, hes_dataset: str, hes_table: Table, target_database: str) -> bool: 
  # Create base dataframe for sampling onto
  base_demographic_df = (
    spark.range(n_patients)
    .select(F.col("id").alias(index_fields[0]))
  )

  # Generate ids for sampling based on the relationships between patients and episodes
  base_demographic_episode_df = (
    relationship_generator(filtered_meta_df, base_demographic_df, variable_fields)
    .select(
      index_fields[0],
      F.col("VALUE_NUMERIC").alias(index_fields[1]),
    )
  )

  # Generate 
  artificial_demographic_df = demographic_field_generator(
    filtered_meta_df, 
    base_demographic_episode_df, 
    "FIELD_NAME", 
    index_fields, 
  )  
  artificial_episode_df = field_generator(filtered_meta_df, base_demographic_episode_df, "FIELD_NAME")
  artificial_df = artificial_demographic_df.join(artificial_episode_df, how="outer", on=index_fields)

  # Postprocessing
  artificial_df = with_hes_id_fields(artificial_df, hes_dataset, hes_table, index_fields)
  derived_fields = get_derivations(hes_dataset)
  artificial_df = with_derived_fields(artificial_df, *derived_fields)
  artificial_df = coerce_hes_schema(artificial_df, hes_dataset)

  # Output 
  success = create_table(
    spark,
    artificial_df, 
    database_name=target_database,
    table_name=ARTIFICIAL_TABLE_NAME_TEMPLATE.format(table_name=hes_table.name),
    format="delta",
    mode="overwrite",
    overwriteSchema="true",  # This doesn't work on ACL clusters!
  )
  
  return success

# COMMAND ----------

meta_table_fullname = f"{meta_database}.artificial_hes_meta"
meta_df = spark.table(meta_table_fullname)

# Generate artificial data for each table
for hes_dataset, hes_dataset_tables in HES_TABLES.items():
  print(f"\nGenerating artificial data for dataset `{hes_dataset}`")
  
  for hes_table in hes_dataset_tables:
    print(f"\tGenerating artificial data representing table `{hes_table.name}` into database `{artificial_database}`")
      
    filtered_meta_df = (
      meta_df
      .filter(F.col("TABLE_NAME") == hes_table.name)
      .drop("TABLE_NAME")
    )
    
    if filtered_meta_df.first() is None:
      print(f"\t\tNo metadata found for table '{hes_table.name}': artificial data table will be empty!")

    success = generate_artificial_hes_table(filtered_meta_df, hes_dataset, hes_table, artificial_database)
    

# COMMAND ----------

notebook_response = {}

# TODO: work into for loop above (e.g. try/except for each table and compile list of fails / successes)
if success:
  notebook_response["status"] = "success"
  notebook_response["message"] = ""
  
else:
  notebook_response["status"] = "failed"
  notebook_response["message"] = "Failed to write test data"
  
dbutils.notebook.exit(json.dumps(notebook_response))

# COMMAND ----------

