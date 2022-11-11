# Databricks notebook source
from typing import List


HES_AE_PRIMARY_KEY = "AEKEY"
HES_AE_TABLE_NAMES = [
    "hes_ae_2021",
    "hes_ae_1920",
    "hes_ae_1819",
    "hes_ae_1718",
    "hes_ae_1617",
    "hes_ae_1516",
    "hes_ae_1415",
    "hes_ae_1314",
    "hes_ae_1213",
    "hes_ae_1112",
    "hes_ae_1011",
    "hes_ae_0910",
    "hes_ae_0809",
    "hes_ae_0708",
]


def _get_hes_ae_key_fields(patient_table: Table) -> List[Field]:
  return [
    Field(HES_AE_PRIMARY_KEY, str, primary=True),
    Field(HES_PATIENT_KEY, str, foreign=patient_table[HES_PATIENT_KEY]),
  ]


def get_hes_ae_tables(patient_table: Table) -> List[Table]:
  hes_ae_key_fields = _get_hes_ae_key_fields(patient_table)
  hes_ae_tables = []

  for table_name in HES_AE_TABLE_NAMES:
    hes_ae_tables.append(Table(table_name, *hes_ae_key_fields))
    
  return hes_ae_tables


HES_AE_TABLES = get_hes_ae_tables(HES_PATIENTS_SYMBOLIC_TABLE)