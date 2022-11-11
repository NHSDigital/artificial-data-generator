# Databricks notebook source
import pathlib

# COMMAND ----------

def get_adg_project_path(project_env: str, project_version: str) -> pathlib.Path:
  adg_project_name = "iuod_artificial_data_generator"
  
  if project_env == "release":
    root_path = pathlib.Path("/Users/admin/releases/code-promotion")
    adg_project_path = root_path / adg_project_name / project_version
  elif project_env == "staging":
    root_path = pathlib.Path("/staging")
    adg_project_path = root_path / adg_project_name
  elif project_env == "dev":
    root_path = pathlib.Path("../")
    adg_project_path = root_path / adg_project_name
    
  return adg_project_path