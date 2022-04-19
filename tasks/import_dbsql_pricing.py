# Databricks notebook source
import pandas as pd

# COMMAND ----------

# Using the current working branch in Databricks Repos - Files in Repos https://docs.databricks.com/repos/index.html#files-in-repos
df = pd.read_csv("../resources/azure_databricks_sql_pricing.csv")

# COMMAND ----------

spark.createDataFrame(df).createOrReplaceTempView("dbsql_pricing")

# COMMAND ----------

# MAGIC %sql select * from dbsql_pricing

# COMMAND ----------

# MAGIC %sql create or replace table unity_information.dbsql_pricing_table as select * from dbsql_Pricing; 

# COMMAND ----------

# MAGIC %sql OPTIMIZE unity_information.dbsql_pricing_table

# COMMAND ----------

# MAGIC %sql vacuum unity_information.dbsql_pricing_table

# COMMAND ----------


