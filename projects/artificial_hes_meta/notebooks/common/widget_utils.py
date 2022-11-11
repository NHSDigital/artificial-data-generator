# Databricks notebook source
import os

# COMMAND ----------

def check_databricks() -> bool:
  """Check whether the code is running in the databricks environment

  Returns:
      bool: indicates whether the current environment is databricks
  """
  return os.environ.get("VIRTUAL_ENV") == "/databricks/python3"


# COMMAND ----------

def get_required_argument(name: str) -> str:
  """Get the value of a dbutils widget, raising an exception if 
  the value is not provided.

  Args:
      name (str): name of the widget to get the value of

  Raises:
      NotImplementedError: running this function outside databricks
      ValueError: if a value is not provided for the widget

  Returns:
      str: value of the widget
  """
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
    