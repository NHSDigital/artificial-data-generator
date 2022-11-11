# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("db", "", "0.1 Project Database")

# COMMAND ----------

# Config
database_name = dbutils.widgets.get("db")
table_name = "user_docs"
table_path = f"{database_name}.{table_name}"

# Check database exists
if spark.sql(f"SHOW DATABASES LIKE '{database_name}'").first() is None:
  # Database not found - exit
  dbutils.notebook.exit({})

# Template variables replaced during build
user_notice_file_name = "artificial_data_user_notice"

# COMMAND ----------

user_notice_html = """{{artificial_data_user_notice.md}}"""

# COMMAND ----------

# Create and upload the docs
docs_data = [
  [user_notice_file_name, user_notice_html]
]
docs_schema = "file_name: string, content_html: string"
docs_df = spark.createDataFrame(docs_data, docs_schema)
(
  docs_df.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable(table_path)
)

# Make sure users can select from the table but not overwrite
if os.getenv("env", "ref") == "ref":
  owner = "data-managers"
else:
  owner = "admin"

spark.sql(f"ALTER TABLE {table_path} OWNER TO `{owner}`")

# Check the uploads
for file_name, content_html in docs_data:
  result_content_html = (
    spark.table(table_path)
    .where(F.col("file_name") == file_name)
    .first()
    .content_html
  ) 
  assert result_content_html == content_html
