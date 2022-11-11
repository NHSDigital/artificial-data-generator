# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will run all the tests!

# COMMAND ----------

test_notebook_paths = [
  "./aggregation_tests/field_summarisers_tests",
  "./aggregation_tests/relationship_summariser_tests",
  "./preprocessing_tests/test_meta_type_classifier",
]

for notebook_path in test_notebook_paths:
  dbutils.notebook.run(notebook_path, 0)

# COMMAND ----------

