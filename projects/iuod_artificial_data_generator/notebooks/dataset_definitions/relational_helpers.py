# Databricks notebook source
# MAGIC %run ./relational

# COMMAND ----------

from typing import Generator

# region Prevent downstream linting highlights
Table = Table
Field = Field
# endregion


def get_parent_table(table: Table):
    for field in table.fields:
        if field.foreign:
            return field.foreign.table
    else:
        return None

      
def get_primary_key(table: Table) -> Field:
  for field in table.fields:
    if field.primary:
      return field
  else:
    return None
  
  
def get_foreign_keys(table: Table) -> Generator[Field, None, None]:
  return filter(lambda field: field.foreign is not None, table.fields)