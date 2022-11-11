# Databricks notebook source
# MAGIC %run ./hes_ae_schema

# COMMAND ----------

# MAGIC %run ./hes_apc_schema

# COMMAND ----------

# MAGIC %run ./hes_op_schema

# COMMAND ----------

from pyspark.sql import types as T

# COMMAND ----------

def get_hes_schema(hes_dataset: str) -> T.StructType:
  if hes_dataset == "hes_ae":
    return get_hes_ae_schema()
  elif hes_dataset == "hes_op":
    return get_hes_op_schema()
  elif hes_dataset == "hes_apc":
    return get_hes_apc_schema()
  else:
    raise NotImplementedError("Valid options for hes_dataset are: hes_apc, hes_op, hes_ae")
    