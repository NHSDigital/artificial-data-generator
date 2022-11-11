# Databricks notebook source
# MAGIC %run ../notebooks/common/table_helpers

# COMMAND ----------

# Source: https://nhsd-git.digital.nhs.uk/data-services/dps/dps-common-code-library/-/blob/develop/nhsdccl/test_helpers.py

from typing import Union, List, Tuple, Callable
from uuid import uuid4
import traceback
import unittest
from functools import wraps

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

assert_raises = unittest.TestCase().assertRaises

# from .util import create_table, table_exists, drop_table


class TemporaryTable:

    def __init__(self, spark: SparkSession, df_or_schema: Union[DataFrame, StructType], db: str, create: bool = True,
                 overwrite: bool = False):
        self._spark = spark
        self._df_or_schema = df_or_schema
        self._db = db
        self._create = create
        self._overwrite = overwrite
        self._mode = "overwrite" if self._overwrite else "append"  # Different to the original because of the `create_table` args in our codebase
        self._name = f'_tmp_{uuid4().hex}'
        self._asset = f'{self._db}.{self._name}'

    def __enter__(self):
        if not self._create:
            return self

        if isinstance(self._df_or_schema, DataFrame):
            df = self._df_or_schema
        elif isinstance(self._df_or_schema, StructType):
            df = self._spark.createDataFrame([], self._df_or_schema)
        else:
            raise TypeError('Given df_or_schema must be a dataframe or a schema.')

        create_table(self._spark, df, self._db, self._name, mode=self._mode)
        assert table_exists(self._spark, self._db, self._name)
        return self

    def __exit__(self, exc_type, exc_value, tb):

        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        if not table_exists(self._spark, self._db, self._name):
            raise AssertionError(f'Table {self._asset} was already dropped')
        drop_table(self._spark, self._db, self._name)
        if table_exists(self._spark, self._db, self._name):
            raise AssertionError(f'Failed to drop table {self._asset}')

    @property
    def name(self):
        return self._name

    @property
    def db(self):
        return self._db


class FunctionPatch:
    """
    This class is a context manager that allows patching of functions "imported" from another notebook using %run.

    The patch function must be at global scope (i.e. top level)

    >>> def mock_type_check(...):
    >>>   ...
    >>>
    >>> with FunctionPatch('type_check', mock_type_check):
    >>>   #do stuff

    """
    def __init__(self, real_func_name: str, patch_func: Callable):
        self._real_func_name = real_func_name
        self._patch_func = patch_func
        self._backup_real_func = None

    def __enter__(self):
        self._backup_real_func = globals()[self._real_func_name]
        globals()[self._real_func_name] = self._patch_func

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        globals()[self._real_func_name] = self._backup_real_func


class FunctionTestSuite:
    """
    This test suite can be used within Databricks notebooks to register and run tests.

    Example usage:
    >>> suite = FunctionTestSuite()
    >>>
    >>> def foo():
    >>>   ...
    >>>
    >>> @suite.add_test
    >>> def test_foo():
    >>>   ...
    >>>
    >>> suite.run()

    """
    def __init__(self):
        self._suite = unittest.TestSuite()
        self._runner = unittest.TextTestRunner()

    def add_test(self, test_func: Callable) -> None:
        """
        Add a test function to the suite.

        Example:
        >>> def foo():
        >>>   ...
        >>>
        >>> @suite.add_test
        >>> def test_foo():
        >>>   ...
        >>>
        """

        @wraps(test_func)
        def clean_up_func():
            result = test_func()
            return result

        test_case = unittest.FunctionTestCase(clean_up_func)
        self._suite.addTest(test_case)

    def run(self):
        """
        Run the tests and prints the output to the console.

        This method can only be called once: further tests will need
        to be assigned to a new object instance. Or the notebook needs to be rerun.
        """
        if not self._runner.run(self._suite).wasSuccessful():
            raise AssertionError()


def create_dataframe(spark, data: List[Tuple], schema: Union[List[str], StructType]) -> DataFrame:
    """
    The allowed type of the data arguments of the spark.createDataFrame function is not correct, so this function
    wraps it to avoid annoying type mismatch warnings in pycharm.

    spark.createDataFrame and this function have exactly the same result.
    """
    # noinspection PyTypeChecker
    return spark.createDataFrame(data, schema)


def with_retry(n_retries: int=1):
  """
  Note: needs to be within add_test scope 
  e.g. the following will work when calling the test via FunctionTestSuite.run()

  >> @my_test_suite.add_test
  >> @with_retry()
  >> def test_func():
  >>   assert True

  but the following will not work this way, because the test function is registered to the FunctionTestSuite before the retry is added
  
  >> @with_retry()
  >> @my_test_suite.add_test
  >> def test_func():
  >>   assert True
  
  """
  def decorator(test_func):
    def test_func_with_retry(*args, **kwargs):
      exception = None
      
      for i in range(n_retries):
        try:
          return test_func(*args, **kwargs)
        except AssertionError as e:
          exception = e
          print(f"AssertionError raised during execution {i+1} of `{test_func.__name__}`, will retry {n_retries - 1 - i} more times")
          continue
      else:
        if exception:
          raise exception
          
    return test_func_with_retry
  return decorator