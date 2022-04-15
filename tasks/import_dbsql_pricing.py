# Databricks notebook source


# COMMAND ----------

spark.read.format("csv").option("header", "true").option("inferSchema","true").load("dbfs:/FileStore/shared_uploads/franco.patano@databricks.com/Untitled_spreadsheet___Sheet1__2_.csv").createOrReplaceTempView("dbsql_pricing")

# COMMAND ----------

# MAGIC %sql select * from dbsql_pricing

# COMMAND ----------

# MAGIC %sql create or replace table unity_information.dbsql_pricing_table as select * from dbsql_Pricing; 

# COMMAND ----------


