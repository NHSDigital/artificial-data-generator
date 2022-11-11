# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will run all the tests!

# COMMAND ----------

test_notebook_paths = [
  "./field_definitions_tests/derivations_tests/age_fields_tests",
  "./field_definitions_tests/derivations_tests/coded_fields_tests",
  "./field_definitions_tests/derivations_tests/derivations_helpers_tests",
  "./field_definitions_tests/derivations_tests/sequential_fields_tests",
  "./field_definitions_tests/id_fields_tests",
  "./demographic_field_generators_tests",
]

for notebook_path in test_notebook_paths:
  dbutils.notebook.run(notebook_path, 0)

# COMMAND ----------

