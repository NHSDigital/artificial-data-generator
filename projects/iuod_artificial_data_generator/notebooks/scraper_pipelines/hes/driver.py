# Databricks notebook source
# MAGIC %run ../../dataset_definitions/hes/hes_tables

# COMMAND ----------

# MAGIC %run ../driver_imports

# COMMAND ----------

# MAGIC %run ./constants/type_overrides

# COMMAND ----------

# MAGIC %run ./constants/excluded_fields

# COMMAND ----------

# MAGIC %run ./constants/disclosure_control_parameters

# COMMAND ----------

# MAGIC %run ./demographic_field_summarisers

# COMMAND ----------

from pprint import pprint
import json
from typing import List, Dict

from pyspark.sql import functions as F


# region Prevent downstream linting highlights
spark = spark
dbutils = dbutils
get_required_argument = get_required_argument
Table = Table
EXCLUDED_FIELDS = EXCLUDED_FIELDS
create_table = create_table
insert_into = insert_into
get_type_overrides = get_type_overrides
table_exists = table_exists
wide_to_long = wide_to_long
get_foreign_keys = get_foreign_keys
scrape_fields = scrape_fields
HES_TABLES = HES_TABLES
HES_FREQUENCY_ROUNDING = HES_FREQUENCY_ROUNDING
HES_MIN_FREQUENCY = HES_MIN_FREQUENCY
get_demographic_summarisers = get_demographic_summarisers
scrape_relationships = scrape_relationships
ingest = ingest
# endregion

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("hes_database",    "hes",                 "0.1 HES DB")
dbutils.widgets.text("meta_database",   "artificial_hes_meta", "0.2 Meta DB")

# COMMAND ----------

# Config
hes_database = get_required_argument("hes_database")
meta_database = get_required_argument("meta_database")

print("Running with parameters: ")
pprint({
  "hes_database": hes_database,
  "meta_database": meta_database,
})

# COMMAND ----------

def scrape_hes_metadata(
  hes_table: Table, 
  source_database: str, 
  target_database: str,
  type_overrides: List[Dict[str, str]], 
  overwrite=False
) -> bool:
  hes_df = ingest(source_database, hes_table.qualifier)
  
  # TODO: explain this
  hes_table_foreign_key = next(get_foreign_keys(hes_table)).name  # PSEUDO_HESID
  included_fields = set(hes_df.columns).difference(EXCLUDED_FIELDS).union([hes_table_foreign_key])
  hes_included_df = hes_df.select(*included_fields)

  field_meta_df = scrape_fields(
    hes_included_df,
    type_overrides, 
    frequency_rounding=HES_FREQUENCY_ROUNDING, 
    min_frequency=HES_MIN_FREQUENCY,
    dataset_summarisers=get_demographic_summarisers(),
    unpivoted_fields=[hes_table_foreign_key]
  )
  
  # Check exclusions
  assert field_meta_df.filter(F.col("FIELD_NAME").isin(EXCLUDED_FIELDS)).first() is None, "Scraped fields which should have been excluded!"
  
  relational_meta_df = scrape_relationships(hes_df, hes_table)
  
  meta_df = field_meta_df.unionByName(relational_meta_df)
  
  # Write output
  output_args = (spark, meta_df, target_database, "artificial_hes_meta")
  if overwrite:
    success = create_table(
      *output_args,
      format="delta",
      mode="overwrite",
      overwriteSchema="true",  # This doesn't work on ACL clusters!
      partitionBy=["TABLE_NAME", "FIELD_NAME"]
    )
  else:
    success = insert_into(*output_args, overwrite=False)
    
  return success


# COMMAND ----------

type_overrides = get_type_overrides()
overwrite = True  # Switch to False after first overwrite so that table gets appended

# Generate artificial data for each table
for hes_dataset, hes_dataset_tables in HES_TABLES.items():
  print(f"\nExtracting metadata for dataset '{hes_dataset}'")
  
  for hes_table in hes_dataset_tables:
    if not table_exists(spark, hes_database, hes_table.name):
      print(f"\tTable `{hes_database}.{hes_table.name}` does not exist: skipping")
      continue
    else:
      print(f"\tExtracting metadata for table `{hes_database}.{hes_table.name}`")
    
    success = scrape_hes_metadata(hes_table, hes_database, meta_database, type_overrides, overwrite=overwrite)
    
    # Switch to prevent overwriting current outputs
    overwrite = False

spark.sql(f"OPTIMIZE {meta_database}.artificial_hes_meta").show() 

# COMMAND ----------

notebook_response = {}

if success:
  notebook_response["status"] = "success"
  notebook_response["message"] = ""

else:
  notebook_response["status"] = "failed"
  notebook_response["message"] = "Failed to write metadata"
  
dbutils.notebook.exit(json.dumps(notebook_response))

# COMMAND ----------

