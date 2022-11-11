# Databricks notebook source
# MAGIC %run ./coded_fields

# COMMAND ----------

# MAGIC %run ./sequential_fields

# COMMAND ----------

# MAGIC %run ./age_fields

# COMMAND ----------

from typing import List, Tuple
from pyspark.sql import Column


def _get_apc_derivations() -> List[Tuple[str, Column]]:
  startage_calc = ("STARTAGE_CALC", get_fractional_age_field("ADMIDATE", mydob_date))
  startage = ("STARTAGE", get_categorized_age_field("ADMIDATE", mydob_date))
  opertn_34_fields = get_opertn_34_fields()
  diag_34_fields = get_diag_34_fields()
  apc_sequential_fields = get_apc_sequential_fields()
  return [
    startage_calc,
    startage,
    *opertn_34_fields,
    *diag_34_fields,
    *apc_sequential_fields,
  ]


def _get_op_derivations() -> List[Tuple[str, Column]]:
  apptage_calc = ("APPTAGE_CALC", get_fractional_from_categorized_age_field("APPTAGE"))
  opertn_34_fields = get_opertn_34_fields()
  diag_34_fields = get_diag_34_fields(limit=12)  # OP only has 12 DIAG fields
  op_sequential_fields = get_op_sequential_fields()
  return [
    apptage_calc,
    *opertn_34_fields,
    *diag_34_fields,
    *op_sequential_fields,
  ]


def _get_ae_derivations() -> List[Tuple[str, Column]]:
  arrivalage_calc = ("ARRIVALAGE_CALC", get_fractional_from_categorized_age_field("ARRIVALAGE"))
  diag2_fields = [(f"DIAG2_{i+1:02}", F.substring(x, 0, 3)) for i, x in enumerate(diag3_fields)]
  treat2_fields = [(f"TREAT2_{i+1:02}", F.substring(x, 0, 3)) for i, x in enumerate(treat3_fields)]
  ae_sequential_fields = get_ae_sequential_fields()
  return [
    *diag2_fields,
    *treat2_fields,
    arrivalage_calc,
    *ae_sequential_fields,
  ]


def get_derivations(dataset: str) -> List[Tuple[str, Column]]:
  derivation_functions = {
    "hes_ae": _get_ae_derivations,
    "hes_apc": _get_apc_derivations,
    "hes_op": _get_op_derivations,
  }
  _get_derivations = derivation_functions.get(dataset, lambda: [])
  derived_fields = _get_derivations()
  return derived_fields


# COMMAND ----------

