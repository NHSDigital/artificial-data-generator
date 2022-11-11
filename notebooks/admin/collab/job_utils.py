# Databricks notebook source
# MAGIC %run ./dotenv.py

# COMMAND ----------

import json
import os
import time
from typing import Dict, Tuple, Any

# From dotenv.py
API_PREFIX = API_PREFIX
API_SUFFIX = API_SUFFIX

DATABRICKS_ENV = os.getenv("env", "dev")
HTTPS = "https://"
PENDING_STATE = "PENDING"
RUNNING_STATE = "RUNNING"


def get_databricks_api_host() -> str:
  """ Get URL for the Databricks API 2.0 for the current environment. 
  
  NOTE: This is specific to NHS Digital infrastructure.

  Returns
  -------
  str
    API base URL for the current environment
    
  """
  host_segments = [f"https://{API_PREFIX}"]
  
  if DATABRICKS_ENV != "prod":
    host_segments.append(DATABRICKS_ENV)
    
  host_segments.append(API_SUFFIX)
  host = ".".join(host_segments)
  
  return host


def configure_databricks_cli(token: str) -> None:
  """ Configure the Databricks CLI with the hostname and token. 

  Parameters
  ----------
  token: str
    User generated access token

  """
  os.environ["DATABRICKS_HOST"] = get_databricks_api_host()
  os.environ["DATABRICKS_TOKEN"] = token
  

def get_job_name_id_map() -> Dict[str, str]:
  result = os.popen("databricks jobs list").read()
  result = result.split("\n")
  result = filter(None, result)
  job_name_id_map = dict(map(lambda s: s.split()[::-1], result))
  return job_name_id_map


def get_job_run_state(run_id: str) -> Dict[str, str]:
  get_run_command = f"databricks runs get --run-id {run_id}"
  result_json = os.popen(get_run_command).read()
  result = json.loads(result_json)
  state = result.get("state", {})
  return state


def wait_for_job_result(run_id: str, sleep_seconds: int=5, max_retries: int=60) -> str:
  print(f"Checking state of run with run_id '{run_id}'")
  
  for i in range(max_retries):
    state = get_job_run_state(run_id)
    life_cycle_state = state.get("life_cycle_state", PENDING_STATE)
    result_state = state.get("result_state", PENDING_STATE)

    if life_cycle_state in (PENDING_STATE, RUNNING_STATE):
      print(f"Run with run_id '{run_id}' incomplete with life_cycle_state '{life_cycle_state}', checking again in {sleep_seconds} seconds")
      time.sleep(sleep_seconds)
    else:
      print(f"Run with run_id '{run_id}' completed with result_state '{result_state}'")
      return result_state
    
  else:
    print(f"Run with run_id '{run_id}' incomplete with life_cycle_state '{life_cycle_state}' but maximum number of retries ({max_retries}) has been exceeded")
    return result_state
  

def run_job_with_notebook_params(job_id: str, notebook_params: str) -> str:
  # Check if there is already a job running
  print(f"Checking state of last run for job with job_id '{job_id}'")
  list_runs_command = f"databricks runs list --job-id {job_id}"
  result = os.popen(list_runs_command).read()

  if result == "\n":
    # No runs yet
    last_run_id = None
    last_run_life_cycle_state = None
  else:
    last_run_id = result.split("\n")[0].split()[0]
    last_run_state = get_job_run_state(last_run_id)
    last_run_life_cycle_state = last_run_state.get("life_cycle_state", PENDING_STATE)
    print(f"Last run for job with job_id '{job_id}' has life_cycle_state '{last_run_life_cycle_state}': run_id '{last_run_id}'")

  if last_run_life_cycle_state in (PENDING_STATE, RUNNING_STATE):
    # Job still pending/running
    return {"run_id": last_run_id}
  else:
    # Start a new job
    run_job_command = f"databricks jobs run-now --job-id {job_id} --notebook-params '{notebook_params}'"
    result_json = os.popen(run_job_command).read()
    result = json.loads(result_json)    
    print(f"Started new job run for job_id '{job_id}' with run_id '{result['run_id']}'")
    return result


def run_job(job_name: str, **job_kwargs) -> str:
  job_name_id_map = get_job_name_id_map()
  job_id = job_name_id_map[job_name]
  print(f"Found job_id for job '{job_name}': '{job_id}'")
  
  # Notebook arguments
  notebook_params = json.dumps(job_kwargs)
  
  return run_job_with_notebook_params(job_id, notebook_params)


def run_job_async(*run_args: Tuple[Any], sleep_seconds: int=30, max_retries: int=20, **run_kwargs) -> Dict[str, str]:
  """ Run a job and keep checking status until it finishes or until a maximum number of retries
  
  Parameters
  ----------
  *run_args: Any
    Passed through to run_job as arguments
  sleep_seconds: int (default=30)
    Number of seconds to sleep after each retry
  max_retries: int (default=20)
    Number of times to retry checking if the run has finished
  **run_kwargs: Any
    Passed through to run_job as key-word arguments
    
  Returns
  -------
  Dict[str, str]
    Result of the job run with the result state
  
  """
  result = run_job(*run_args, **run_kwargs)
  run_id = result["run_id"]
  result_state = wait_for_job_result(run_id, sleep_seconds=sleep_seconds, max_retries=max_retries)
  return {**result, "result_state": result_state}
