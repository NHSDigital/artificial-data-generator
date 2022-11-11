# Databricks notebook source
import os
import pprint
from typing import Iterable
from functools import reduce
from py4j.protocol import Py4JError
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


def drop_table(spark: SparkSession, db: str, table: str):
  """Drop a spark sql table

  Args:
      spark (pyspark.sql.SparkSession): spark session
      db (str): the name of the database containing the table
      table (str): the name of the table to drop
  """
  spark.sql(f"DROP TABLE {db}.{table}")


def table_exists(spark: SparkSession, db: str, table: str) -> bool:
  """Check if a table exists in a database

  Args:
      spark (pyspark.sql.SparkSession): spark session
      db (str): the name of the database to search
      table (str): the name of the table to search for

  Returns:
      bool: whether the table exists in the database
  """
  return spark.sql(F"SHOW TABLES IN {db}").filter(F.col("tableName") == table).first() is not None


def database_exists(spark: SparkSession, database_name: str, verbose: bool = False) -> bool:
  """Check if a database exists / is accessible within the current session

  Args:
      spark (spark.sql.SparkSession): spark session
      database_name (str): the database to look for
      verbose (bool, optional): whether to include additional printed output
      if the check returns False. Defaults to False.

  Returns:
      bool: indicates whether the database exists within the current scope
  """
  databases = spark.sql("SHOW DATABASES").collect()
  databases = [db.databaseName for db in databases]
  exists = database_name in databases
  
  if not exists and verbose:
    print(f"Database '{database_name}' not found.")
    print("Available databases:")
    pprint.pprint(databases)
    
  return exists
  

def get_table_owner(spark: SparkSession, database_name: str, table_name: str) -> str:
  """Get the name of the owner of a spark sql table

  Args:
      spark (spark.sql.SparkSession): spark session
      database_name (str): the database containing the table
      table_name (str): the table to find the owner of

  Returns:
      str: the name of the table's owner
  """
  return (
    spark.sql(f"SHOW GRANT ON {database_name}.{table_name}")
    .where("ActionType == 'OWN'")
    .first()
    .Principal
  )
  
  
def set_table_owner(spark: SparkSession, database_name: str, table_name: str, role: str):
  """Modify the table owner. Note: this does not first check if the current user 
  has priveleges to modify the owner - if they don't it will fail.

  Args:
      spark (spark.sql.SparkSession): spark session
      database_name (str): name of the database containing the table
      table_name (str): name of the table to modify
      role (str): role / user name of the new owner
  """
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO `{role}`")
    

def create_table(
  spark: SparkSession, 
  df: DataFrame, 
  database_name: str, 
  table_name: str, 
  **writer_options
) -> bool:
  """ Write a dataframe to a table and set the owner according to the 
  environment (this is intended for use by code promotion projects in NHS Digital DAE/DPS)

  Args:
      spark (spark.sql.SparkSession): spark session
      df (spark.sql.DataFrame): dataframe to write
      database_name (str): name of the database to write the table in
      table_name (str): name of the table to be created

  Returns:
      bool: whether the operation was a success
  """
  table_fullname = f"{database_name}.{table_name}"
  mode = writer_options.pop("mode", "append")
  format_ = writer_options.pop("format", "delta")
  partition_by = writer_options.pop("partitionBy", [])
  
  (
    df
    .write
    .format(format_)
    .mode(mode)
    .partitionBy(*partition_by)
    .options(**writer_options)
    .saveAsTable(table_fullname)
  )

  # In dev / prod, set the owner to 'admin'
  # In ref, which is actually our dev environment, set it to 'data-managers'
  # This lets us modify the tables in ref after they are created
  # TODO: we should set up some test databases and then change this code based on the database we're targetting
  if os.getenv("env", "ref") == "ref":
    target_owner = "data-managers"
  else:
    target_owner = "admin"
      
  if get_table_owner(spark, database_name, table_name) != target_owner:
    try:
      set_table_owner(spark, database_name, table_name, target_owner)
      print(f"Owner for table `{database_name}.{table_name}` changed to {target_owner}")
    except Py4JError as e:
      print(f"Exception raised while attempting to set table owner to {target_owner}")
      print(e)
      
  return True


def create_table_from_schema(
  spark: SparkSession, 
  database_name: str, 
  table_name: str, 
  schema: T.StructType, 
  **writer_options
) -> None:
  """Create an empty table in a database with a given schema. See notes
  on create_table which is called by this function.

  Args:
      spark (spark.sql.SparkSession): spark session
      database_name (str): name of the database to write the table in
      table_name (str): name of the table to be created
      schema (spark.sql.types.StructType): schema for the table to be created

  Returns:
      _type_: _description_
  """
  schema_df = spark.createDataFrame([], schema)
  return create_table(
    spark, 
    schema_df, 
    database_name, 
    table_name, 
    **writer_options
  )


def insert_into(
  spark: SparkSession, 
  df: DataFrame, 
  database_name: str, 
  table_name: str, 
  overwrite: bool=False
) -> bool:
  """Insert records from a dataframe into an existing spark sql table

  Args:
      spark (pyspark.sql.SparkSession): spark session
      df (pyspark.sql.DataFrame): dataframe containing records to insert
      database_name (str): name of the database containing the table to be modified
      table_name (str): name of the table to insert the records into
      overwrite (bool, optional): passed to DataFrame.insertInto, indicates
      whether the records should be appended or overwritten. Defaults to False.

  Returns:
      bool: whether the operation was a success
  """
  table_fullname = f"{database_name}.{table_name}"

  if not table_exists(spark, database_name, table_name):
    print(f"Table '{table_fullname}' does not exist!")
    return False

  df.write.insertInto(table_fullname, overwrite=overwrite)
  
  return True