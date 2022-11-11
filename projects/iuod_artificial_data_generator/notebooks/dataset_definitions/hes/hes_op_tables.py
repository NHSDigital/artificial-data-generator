# Databricks notebook source
from typing import List


HES_OP_PRIMARY_KEY = "ATTENDKEY"
HES_OP_TABLE_NAMES = [
    "hes_op_2021",
    "hes_op_1920",
    "hes_op_1819",
    "hes_op_1718",
    "hes_op_1617",
    "hes_op_1516",
    "hes_op_1415",
    "hes_op_1314",
    "hes_op_1213",
    "hes_op_1112",
    "hes_op_1011",
    "hes_op_0910",
    "hes_op_0809",
    "hes_op_0708",
    "hes_op_0607",
    "hes_op_0506",
    "hes_op_0405",
    "hes_op_0304",
]


def _get_hes_op_key_fields(patient_table: Table) -> List[Field]:
  return [
    Field(HES_OP_PRIMARY_KEY, str, primary=True),
    Field(HES_PATIENT_KEY, str, foreign=patient_table[HES_PATIENT_KEY]),
  ]


def get_hes_op_tables(patient_table: Table) -> List[Table]:
  hes_op_key_fields = _get_hes_op_key_fields(patient_table)
  hes_op_tables = []

  for table_name in HES_OP_TABLE_NAMES:
    hes_op_tables.append(Table(table_name, *hes_op_key_fields))
    
  return hes_op_tables


HES_OP_TABLES = get_hes_op_tables(HES_PATIENTS_SYMBOLIC_TABLE)