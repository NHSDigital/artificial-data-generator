# Databricks notebook source
import pathlib

ADG_PROJECT_VERSION = "4497+20221019131315.git06db5e6f6"
ADG_PROJECT_NAME = "iuod_artificial_data_generator"
CODE_PROMOTION_RELEASES_PATH = pathlib.Path("/Users/admin/releases/code-promotion")


def get_adg_env(project_version: str) -> str:
  if project_version == "dev":
    return "dev"
  elif project_version == "staging":
    return "staging"
  else:
    return "release"  


def get_adg_project_path(project_version: str) -> pathlib.Path:
  adg_env = get_adg_env(project_version)
  
  if adg_env == "release":
    adg_project_path = CODE_PROMOTION_RELEASES_PATH / ADG_PROJECT_NAME / ADG_PROJECT_VERSION
  elif adg_env == "staging":
    root_path = pathlib.Path("/staging")
    adg_project_path = root_path / ADG_PROJECT_NAME
  elif adg_env == "dev":
    root_path = pathlib.Path("../")
    adg_project_path = root_path / ADG_PROJECT_NAME
    
  return adg_project_path