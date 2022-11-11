# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will run all the tests! 

# COMMAND ----------

test_notebook_paths = [
  "./field_generators_tests",
  "./relationship_generator_tests",
  "./sampling_tests/field_definitions_tests",
]

for notebook_path in test_notebook_paths:
  dbutils.notebook.run(notebook_path, 0)

# COMMAND ----------

