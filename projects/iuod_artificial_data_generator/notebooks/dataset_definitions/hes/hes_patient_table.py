# Databricks notebook source
# MAGIC %run ../relational

# COMMAND ----------

# from dsp.common.relational import Table, Field

HES_PATIENT_KEY = "PSEUDO_HESID"


def get_hes_patients_symbolic_table() -> Table:
  """
  Note
  ----
  This function returns an object that symbolises the fact that there is 
  no 'Master' table in HES. Patient IDs are duplicated across events, but in 
  principle they are unique for each individual and the distinct values
  could be extracted into a table which represents the unique patients within HES.
  
  We don't actually need to create the table, just to be aware 
  that the primary key on this symbolic table is the foreign key on the 
  actual hes tables used to specify the patient to which an episode relates
  """
  fields = [
    Field(HES_PATIENT_KEY, str, primary=True),
    Field("MYDOB", str),
    Field("SEX", str),
    Field("ETHNOS", str),
  ]
  return Table("hes_patients_symbolic", *fields)


HES_PATIENTS_SYMBOLIC_TABLE = get_hes_patients_symbolic_table()