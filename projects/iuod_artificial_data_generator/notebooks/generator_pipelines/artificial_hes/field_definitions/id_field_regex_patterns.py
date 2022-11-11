# Databricks notebook source
ID_FIELD_PATTERNS = dict(
  PSEUDO_HESID = r"TEST[0-9a-zA-Z]{28}",  # 32an - first 4 chars = TEST to ensure no overlap with real IDs
  SUSRECID     = r"\d{14}",
  AEKEY        = r"\d{12}",               # Changes to r"\d{20}" in 2021/22
  EPIKEY       = r"\d{12}",               # Changes to r"\d{20}" in 2021/22
  ATTENDNO     = r"[0-9a-zA-Z]{12}",
  ATTENDKEY    = r"\d{12}",               # Changes to r"\d{20}" in 2021/22
  ATTENDID     = r"[0-9a-zA-Z]{12}",
  PREFERER     = r"[0-9a-zA-Z]{16}",      # What about nulls (&) / invalids (99)
)