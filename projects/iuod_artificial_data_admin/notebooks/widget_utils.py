# Databricks notebook source
import os

# COMMAND ----------

def check_databricks() -> bool:
  return os.environ.get("VIRTUAL_ENV") == "/databricks/python3"


# COMMAND ----------

def get_required_argument(name: str) -> str:
  if check_databricks():
    value = dbutils.widgets.get(name)
  else:
    # TODO
    raise NotImplementedError("No way to provide arguments outside of Databricks")
    
  try:
    assert value
    return value
  except AssertionError:
    raise ValueError(f"Argument '{name}' must be provided")
    