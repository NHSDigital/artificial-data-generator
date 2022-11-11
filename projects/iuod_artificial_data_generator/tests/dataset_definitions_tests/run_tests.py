# Databricks notebook source
# MAGIC %md
# MAGIC This notebook will run all the tests!

# COMMAND ----------

test_notebook_paths = []

for notebook_path in test_notebook_paths:
  dbutils.notebook.run(notebook_path, 0)