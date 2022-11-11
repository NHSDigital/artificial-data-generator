# Databricks notebook source
# MAGIC %run ./hes_patient_table

# COMMAND ----------

# MAGIC %run ./hes_ae_tables

# COMMAND ----------

# MAGIC %run ./hes_apc_tables

# COMMAND ----------

# MAGIC %run ./hes_op_tables

# COMMAND ----------

HES_TABLES = {
    "hes_ae": HES_AE_TABLES,
    "hes_apc": HES_APC_TABLES,
    "hes_op": HES_OP_TABLES,
}