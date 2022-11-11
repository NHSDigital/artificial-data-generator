from pyspark.sql import functions as F

# Config
database_name = "artificial_hes"
table_name = "user_docs"
table_path = f"{database_name}.{table_name}"

# Get the content
content_html = (
  spark.table(table_path)
  .where(F.col("file_name") == "artificial_data_user_notice")
  .first()
  .content_html
)

displayHTML(content_html)
