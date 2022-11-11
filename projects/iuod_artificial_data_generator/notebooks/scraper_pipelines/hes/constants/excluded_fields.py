# Databricks notebook source
ID_FIELDS = [
  "PSEUDO_HESID",
  "SUSRECID",
  "AEKEY",
  "EPIKEY",
  "ATTENDNO",
  "ATTENDKEY",
  "ATTENDID",
  "PREFERER",
]

OTHER_SENSITIVE_FIELDS = []

DERIVED_FIELDS = [
  *[f"OPERTN_3_{i+1:02}" for i in range(24)],  # Derived from OPERTN_4 (APC & OP)
  *[f"DIAG_3_{i+1:02}" for i in range(20)],    # Derived from DIAG_4 (APC & OP)
  *[f"DIAG2_{i+1:02}" for i in range(12)],     # Derived from DIAG3 (AE)
  *[f"TREAT2_{i+1:02}" for i in range(12)],    # Derived from TREAT3 (AE)
  "OPERTN_3_CONCAT",
  "OPERTN_4_CONCAT",
  "OPERTN_COUNT",
  "DIAG_3_CONCAT",
  "DIAG_4_CONCAT",
  "DIAG_COUNT",
  "STARTAGE_CALC",   # Derived from MYDOB and ADMIDATE (APC)
  "STARTAGE",        # Derived from MYDOB and ADMIDATE (APC)
  "APPTAGE_CALC",    # Can't be derived from MYDOB because this isn't in HES Non Sensitive! Derive from APPTAGE (OP)
  # "APPTAGE",       # Can't be derived from MYDOB because this isn't in HES Non Sensitive! (OP)
  "ARRIVALAGE_CALC", # Can't be derived from MYDOB because this isn't in HES Non Sensitive! Derive from ARRIVALAGE (AE)
  # "ARRIVALAGE",    # Can't be derived from MYDOB because this isn't in HES Non Sensitive! (AE)
]

EXCLUDED_FIELDS = [
  *ID_FIELDS,
  *OTHER_SENSITIVE_FIELDS,
  *DERIVED_FIELDS,
]

# COMMAND ----------

