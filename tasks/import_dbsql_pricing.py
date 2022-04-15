# Databricks notebook source
import pandas as pd


# COMMAND ----------

df = pd.read_csv("/Workspace/Repos/franco.patano@databricks.com/lakehouse_dba_tools/resources/azure_databricks_sql_pricing.csv")

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


