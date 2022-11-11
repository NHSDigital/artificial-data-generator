# Databricks notebook source
# NOTE: "RTTPERSTART" & "RTTPEREND" appear in data dictionary but not APC, OP, AE tables for DAE version of the HES asset
APC_SEQUENTIAL_DATE_FIELDS = ["ELECDATE", "ADMIDATE", "EPISTART", "EPIEND", "DISDATE"]  # TODO: confirm the ordering
AE_SEQUENTIAL_DATE_FIELDS = []
OP_SEQUENTIAL_DATE_FIELDS = []