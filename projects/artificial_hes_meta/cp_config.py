# Databricks notebook source
# MAGIC %md
# MAGIC # Code Promotion Job Configuration
# MAGIC ### This notebook sets some parameters for Databricks jobs wrapping the top-level entry point notebooks.
# MAGIC Only simple setting of variables is allowed in this notebook.

# COMMAND ----------

import os

# COMMAND ----------

# DBTITLE 1,Global settings
# MAGIC %md
# MAGIC spark_version can be either "6.6.x-scala2.11" (spark 2) or "9.1.x-scala2.12" (spark 3).   
# MAGIC This applies to all jobs created

# COMMAND ----------

spark_version = "6.6.x-scala2.11"

# COMMAND ----------

# DBTITLE 1,init_schemas
# MAGIC %md
# MAGIC Available parameters:
# MAGIC  - **retain_cluster**: boolean flag to indicate if existing cluster definition for the job is retained

# COMMAND ----------

# Example:
init_schemas = {
    "retain_cluster": False
}

# COMMAND ----------

# DBTITLE 1,run_notebooks
# MAGIC %md
# MAGIC Available parameters:
# MAGIC  - **concurrency**: Integer between 1 and 10. Allows you to run multiple *run_notebooks* jobs at the same time. 
# MAGIC  - **extra_parameters**: Dictionary(String, String) that maps *parameter names* to *default values*. These parameters are added to the list of parameters for the job.  
# MAGIC  - **schedule**: A quartz cron syntax on when to run - see https://www.freeformatter.com/cron-expression-generator-quartz.html for cron syntax.   
# MAGIC  - **retain_cluster**: boolean flag to indicate if existing cluster definition for the job is retained

# COMMAND ----------

# Example:
run_notebooks = {
    "concurrency": 1,
    "extra_parameters": {},
    "retain_cluster": False,
}

if os.getenv("env", "ref") == "prod":
    run_notebooks = {
        **run_notebooks,
        "num_workers": 8,
        "instance_type": "i3.2xlarge",
    }

# COMMAND ----------

# DBTITLE 1,tool_config
# MAGIC %md
# MAGIC Available parameters:
# MAGIC  - **retain_cluster**: boolean flag to indicate if existing cluster definition for the job is retained

# COMMAND ----------

# Example:
# tool_config = {
#  "retain_cluster": True
# }
#tool_config = {
#}