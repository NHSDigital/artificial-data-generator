# Databricks notebook source
# MAGIC %md ### README
# MAGIC This notebook will run all the tests! Each test notebook is run in turn and the results are printed out in the cell below the run. New test notebooks should be added to the runs. 
# MAGIC 
# MAGIC Test failures will be raised as an error in the final cell of this notebook (i.e. all the tests will run first, regardless of passes or failures, and then there is a final step to check how many failures there were and alert the user). This should avoid failing tests remaining undetected.

# COMMAND ----------

from typing import Generator, Union, Tuple, Dict
from pyspark.util import Py4JJavaError


def get_test_runner() -> Generator[Dict[str, Union[None, AssertionError]], Tuple[str, str], None]:
  errors = {}
  
  while True:
    test_name, test_path = yield
    
    try:
      dbutils.notebook.run(test_path, 0)
      
      if test_name in errors:
        del errors[test_name]
    except Py4JJavaError as e:
      errors[test_name] = e
      
    yield errors

    
def run_tests(tests):
  test_runner = get_test_runner()
  errors = test_runner.send(None)

  for test_name, test_path in tests.items():
    errors = test_runner.send((test_name, test_path))
    next(test_runner)

  if errors:
    for test_name, e in errors.items():
      print(f'Test failures detected in {tests[test_name]}: {e}')

# COMMAND ----------

tests = {
  "scraper_stages": "./tests/scraper_stages_tests/run_tests",
  "generator_stages": "./tests/generator_stages_tests/run_tests",
  "scraper_pipelines": "./tests/scraper_pipelines_tests/run_tests",
  "generator_pipelines": "./tests/generator_pipelines_tests/run_tests",
  "dataset_definitions": "./tests/dataset_definitions_tests/run_tests",
  "common": "./tests/common_tests/run_tests",
}

run_tests(tests)

# COMMAND ----------

