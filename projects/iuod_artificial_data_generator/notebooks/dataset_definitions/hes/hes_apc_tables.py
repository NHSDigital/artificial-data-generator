# Databricks notebook source
from typing import List


HES_APC_PRIMARY_KEY = "EPIKEY"
HES_APC_TABLE_NAMES = [
    "hes_apc_2021",
    "hes_apc_1920",
    "hes_apc_1819",
    "hes_apc_1718",
    "hes_apc_1617",
    "hes_apc_1516",
    "hes_apc_1415",
    "hes_apc_1314",
    "hes_apc_1213",
    "hes_apc_1112",
    "hes_apc_1011",
    "hes_apc_0910",
    "hes_apc_0809",
    "hes_apc_0708",
    "hes_apc_0607",
    "hes_apc_0506",
    "hes_apc_0405",
    "hes_apc_0304",
    "hes_apc_0203",
    "hes_apc_0102",
]


def _get_hes_apc_key_fields(patient_table: Table) -> List[Field]:
  return [
    Field(HES_APC_PRIMARY_KEY, str, primary=True),
    Field(HES_PATIENT_KEY, str, foreign=patient_table[HES_PATIENT_KEY]),
  ]


def get_hes_apc_tables(patient_table: Table) -> List[Table]:
  hes_apc_key_fields = _get_hes_apc_key_fields(patient_table)
  hes_apc_tables = []

  for table_name in HES_APC_TABLE_NAMES:
    hes_apc_tables.append(Table(table_name, *hes_apc_key_fields))
    
  return hes_apc_tables


HES_APC_TABLES = get_hes_apc_tables(HES_PATIENTS_SYMBOLIC_TABLE)