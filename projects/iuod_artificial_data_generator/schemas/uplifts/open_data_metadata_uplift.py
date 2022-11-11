# Databricks notebook source
# MAGIC %md
# MAGIC # Open Data Metadata Uplift
# MAGIC 
# MAGIC This notebook uplifts the schema of a table containing metadata to greatly improve adherance to [NHS Digital's Open Data standards](https://github.com/NHS Digitaligital/open-data-standards).

# COMMAND ----------

# MAGIC %md ## Setup, Imports & Widgets

# COMMAND ----------

# MAGIC %run ../../notebooks/common/common_exports

# COMMAND ----------

# MAGIC %run ../../notebooks/scraper_stages/schemas/meta_schema

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("database_name", "iuod_artificial_data_generator", "0.1 Database Name")
dbutils.widgets.text("table_name",    "",                               "0.2 Metadata Table Name")

# COMMAND ----------

import json
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

# MAGIC %md ### SQL Expressions

# COMMAND ----------

# Template for transforming the metadata
select_open_data_metadata_template_expr = """
SELECT 
  TABLE_NAME
  ,FIELD_NAME
  ,coalesce(
    CASE 
      WHEN METADATA.SUMMARY.CATEGORICAL.VALUE_TYPE == 'FLAT' THEN 'CATEGORICAL'
      WHEN METADATA.SUMMARY.CATEGORICAL.VALUE_TYPE == 'NESTED' THEN 'DEMOGRAPHIC_CATEGORICAL'
      WHEN FIELD_NAME == 'MYDOB' THEN 'DEMOGRAPHIC_DATE'
    END,
    METADATA.SUMMARY.VALUE_TYPE, 
    METADATA.VALUE_TYPE
  ) AS VALUE_TYPE
  ,coalesce(
    METADATA.FIELD_TYPE, 
    METADATA.SUMMARY.CATEGORICAL.FLAT.VALUE, 
    METADATA.SUMMARY.DATE.FORMAT
   ) AS VALUE_STRING
  ,coalesce(
    METADATA.SUMMARY.DISCRETE.VALUE, 
    METADATA.RELATIONSHIP.MAPPING
  ) AS VALUE_NUMERIC
  ,METADATA.SUMMARY.DATE.VALUE AS VALUE_DATE
  ,coalesce(
    METADATA.SUMMARY.CATEGORICAL.NESTED.VALUE.VALUE,
    METADATA.SUMMARY.DATE.NOISE.REPLACEMENT
  ) as VALUE_STRING_ARRAY
  ,cast(null as array<double>) as VALUE_NUMERIC_ARRAY
  ,coalesce(
    METADATA.SUMMARY.CATEGORICAL.FLAT.WEIGHT,
    METADATA.SUMMARY.CATEGORICAL.NESTED.WEIGHT,
    METADATA.SUMMARY.DATE.WEIGHT,
    METADATA.SUMMARY.DISCRETE.WEIGHT,
    METADATA.RELATIONSHIP.FREQUENCY
  ) AS WEIGHT
  ,coalesce(
    METADATA.SUMMARY.CATEGORICAL.NESTED.VALUE.WEIGHT,
    METADATA.SUMMARY.DATE.NOISE.WEIGHT
  ) AS WEIGHT_ARRAY
FROM {db}.{table} -- Template parameters
WHERE NOT METADATA.SUMMARY.VALUE_TYPE <=> 'CONTINUOUS'
UNION
SELECT 
  TABLE_NAME
  ,FIELD_NAME
  ,METADATA.SUMMARY.VALUE_TYPE
  ,cast(null AS string) AS VALUE_STRING
  ,cast(null AS double) AS VALUE_NUMERIC
  ,cast(null AS date) AS VALUE_DATE
  ,cast(null AS array<string>) AS VALUE_STRING_ARRAY
  ,array_sort(collect_list(METADATA.SUMMARY.CONTINUOUS.VALUE)) AS VALUE_NUMERIC_ARRAY
  ,cast(null AS double) AS WEIGHT
  ,cast(null AS array<double>) AS WEIGHT_ARRAY
FROM {db}.{table} -- Template parameters
WHERE METADATA.SUMMARY.VALUE_TYPE <=> 'CONTINUOUS'
GROUP BY TABLE_NAME, FIELD_NAME, METADATA.SUMMARY.VALUE_TYPE
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

db = get_required_argument("database_name")
target_table = get_required_argument("table_name")
backup_table = f"{target_table}_v1_schema_backup"

print(f"Uplifting schema of table {db}.{target_table}")

# COMMAND ----------

if not database_exists(spark, db, verbose=True):
  dbutils.notebook.exit(json.dumps({"status": "skipped", "message": f"Database {db} not found"}))
  
if not table_exists(spark, db, target_table):
  dbutils.notebook.exit(json.dumps({"status": "skipped", "message": f"Table {db}.{target_table} does not exist"}))
  
source_df = spark.table(f"{db}.{target_table}")
source_schema = T.StructType(sorted(source_df.schema.fields, key=lambda f: f.name))
target_schema = T.StructType(sorted(get_meta_schema().fields, key=lambda f: f.name))

if not datatypes_equal(target_schema, source_schema):
  print(f"Schema of table {db}.{target_table} not equal to target schema")

  # Can't rename tables, so need to backup first and then drop to reuse the name
  print(f"Backing up table {db}.{target_table} to table {db}.{backup_table}")
  create_table(spark, source_df, db, backup_table, partitionBy=["TABLE_NAME", "FIELD_NAME"])
  drop_table(spark, db, target_table)

  print(f"Recreating table {db}.{target_table} from backup with target schema")
  select_open_data_metadata_expr = select_open_data_metadata_template_expr.format(db=db, table=backup_table)
  target_df = spark.sql(select_open_data_metadata_expr)
  create_table(spark, target_df, db, target_table, partitionBy=["TABLE_NAME", "FIELD_NAME"])
  dbutils.notebook.exit(json.dumps({"status": "success", "message": ""}))
  
else:
  dbutils.notebook.exit(json.dumps({"status": "skipped", "message": f"Schema of table {db}.{target_table} already equal to target schema"}))
  

# COMMAND ----------

