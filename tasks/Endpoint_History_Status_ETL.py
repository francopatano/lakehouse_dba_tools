# Databricks notebook source
# MAGIC %sql set spark.databricks.delta.schema.autoMerge.enabled = true

# COMMAND ----------

dbutils.widgets.text('host_name','https://adb-2290777133481849.9.azuredatabricks.net','Host Name')

# COMMAND ----------

# DBTITLE 1,Constants
# If you want to run this notebook yourself, you need to create a Databricks personal access token,
# store it using our secrets API, and pass it in through the Spark config, such as this:
# spark.pat_token {{secrets/query_history_etl/user}}, or Azure Keyvault.

WORKSPACE_HOST = dbutils.widgets.get('host_name')
ENDPOINTS_URL = "{0}/api/2.0/sql/endpoints".format(WORKSPACE_HOST)

DATABASE_NAME = "unity_information"
ENDPOINTS_TABLE_NAME = "endpoints"
QUERIES_TABLE_NAME = "endpoint_queries"
ENDPOINT_HISTORY_STATUS_TABLE = "endpoint_status_history"

#Databricks secrets API
#auth_header = {"Authorization" : "Bearer " + spark.conf.get("spark.pat_token")}
#Azure KeyVault
auth_header = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "etl", key = "dbsql")}

# COMMAND ----------

# DBTITLE 1,Imports
import requests
from datetime import date, datetime, timedelta
from pyspark.sql.functions import from_unixtime, lit, json_tuple
from delta.tables import *
import time

# COMMAND ----------

# DBTITLE 1,Functions definition
def check_table_exist(db_tbl_name):
    table_exist = False
    try:
        spark.read.table(db_tbl_name) # Check if spark can read the table
        table_exist = True        
    except:
        pass
    return table_exist
  
def current_time_in_millis():
    return round(time.time() * 1000)
  
def get_boolean_keys(arrays):
  # A quirk in Python's and Spark's handling of JSON booleans requires us to converting True and False to true and false
  boolean_keys_to_convert = []
  for array in arrays:
    for key in array.keys():
      if type(array[key]) is bool:
        boolean_keys_to_convert.append(key)
  #print(boolean_keys_to_convert)
  return boolean_keys_to_convert

# COMMAND ----------

# DBTITLE 1,Initialization
# notebook_start_execution_time = current_time_in_millis()

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(DATABASE_NAME))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Fetch from SQL Endpoint API
response = requests.get(ENDPOINTS_URL, headers=auth_header)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()

endpoints_json = response_json["endpoints"]

# A quirk in Python's and Spark's handling of JSON booleans requires us to converting True and False to true and false
boolean_keys_to_convert = set(get_boolean_keys(endpoints_json))

for endpoint_json in endpoints_json:
  for key in boolean_keys_to_convert:
    endpoint_json[key] = str(endpoint_json[key]).lower()

endpoints = spark.read.json(sc.parallelize(endpoints_json))

display(endpoints)
endpoints.createOrReplaceTempView("endpoints_status_2")
# endpoints.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(DATABASE_NAME + "." + ENDPOINTS_TABLE_NAME)

# COMMAND ----------

# spark.sql("select *, current_timestamp() polled_timestamp from endpoints_status_2 where state = 'RUNNING'").createOrReplaceTempView("insert_endpoint_status")

# COMMAND ----------

# spark.sql("describe unity_information.endpoint_status_history").createOrReplaceTempView("desc_unity_information_endpoint_status_history")
# spark.sql("select * from desc_unity_information_endpoint_status_history where data_type <> ''").createOrReplaceTempView("desc_unity_information_endpoint_status_history")

# spark.sql("describe insert_endpoint_status").createOrReplaceTempView("insert_endpoint_status_desc")
# spark.sql("select * from insert_endpoint_status_desc where data_type <> ''").createOrReplaceTempView("insert_endpoint_status_desc")


# COMMAND ----------

# list = spark.sql("select distinct d.col_name dist_col from desc_unity_information_endpoint_status_history d  inner join insert_endpoint_status_desc s on s.col_name = d.col_name").collect()[0][0]

# COMMAND ----------

# cols_matched = spark.sql(f"""
# select distinct d.col_name from desc_unity_information_endpoint_status_history d  inner join insert_endpoint_status_desc s on s.col_name = d.col_name
# """).toPandas()['col_name'].tolist()


# COMMAND ----------

# columns_to_update = "', '".join(cols_matched)

# COMMAND ----------



# spark.sql("select *, current_timestamp() polled_timestamp from endpoints_status_2").createOrReplaceTempView("insert_endpoint_status")

# spark.sql("describe unity_information.endpoint_status_history").createOrReplaceTempView("desc_unity_information_endpoint_status_history")
# spark.sql("select * from desc_unity_information_endpoint_status_history where data_type <> ''").createOrReplaceTempView("desc_unity_information_endpoint_status_history")

# spark.sql("describe insert_endpoint_status").createOrReplaceTempView("insert_endpoint_status_desc")
# spark.sql("select * from insert_endpoint_status_desc where data_type <> ''").createOrReplaceTempView("insert_endpoint_status_desc")

# list = spark.sql("select distinct d.col_name dist_col from desc_unity_information_endpoint_status_history d  inner join insert_endpoint_status_desc s on s.col_name = d.col_name").collect()[0][0]


# cols_matched = spark.sql(f"""
# select distinct if(isnull(s.col_name),"null as ","") || d.col_name as col_name , d.col_name dest_cols from desc_unity_information_endpoint_status_history d  full join insert_endpoint_status_desc s on s.col_name = d.col_name order by 2
# """).toPandas()['col_name'].tolist()

# columns_to_update = ", ".join(cols_matched)

# cols_dest = spark.sql(f"""
# select distinct if(isnull(s.col_name),"'' as ","") || d.col_name as col_name ,d.col_name dest_cols from desc_unity_information_endpoint_status_history d  full join insert_endpoint_status_desc s on s.col_name = d.col_name order by 2
# """).toPandas()['dest_cols'].tolist()

# columns_in_destination = ", ".join(cols_dest)

# print(f"insert into unity_information.endpoint_status_history ({columns_in_destination}) select {columns_to_update} from insert_endpoint_status ")


# COMMAND ----------

if check_table_exist("unity_information.endpoint_status_history"):
  spark.sql("select *, current_timestamp() polled_timestamp from endpoints_status_2").createOrReplaceTempView("insert_endpoint_status")

  spark.sql("describe unity_information.endpoint_status_history").createOrReplaceTempView("desc_unity_information_endpoint_status_history")
  spark.sql("select * from desc_unity_information_endpoint_status_history where data_type <> ''").createOrReplaceTempView("desc_unity_information_endpoint_status_history")

  spark.sql("describe insert_endpoint_status").createOrReplaceTempView("insert_endpoint_status_desc")
  spark.sql("select * from insert_endpoint_status_desc where data_type <> ''").createOrReplaceTempView("insert_endpoint_status_desc")

  list = spark.sql("select distinct d.col_name dist_col from desc_unity_information_endpoint_status_history d  inner join insert_endpoint_status_desc s on s.col_name = d.col_name").collect()[0][0]


  cols_matched = spark.sql(f"""
  select distinct if(isnull(s.col_name),"null as ","") || d.col_name as col_name , d.col_name dest_cols from desc_unity_information_endpoint_status_history d  full join insert_endpoint_status_desc s on s.col_name = d.col_name order by 2
  """).toPandas()['col_name'].tolist()

  columns_to_update = ", ".join(cols_matched)

  cols_dest = spark.sql(f"""
  select distinct if(isnull(s.col_name),"'' as ","") || d.col_name as col_name ,d.col_name dest_cols from desc_unity_information_endpoint_status_history d  full join insert_endpoint_status_desc s on s.col_name = d.col_name order by 2
  """).toPandas()['dest_cols'].tolist()

  columns_in_destination = ", ".join(cols_dest)

  spark.sql(f"insert into unity_information.endpoint_status_history ({columns_in_destination}) select {columns_to_update} from insert_endpoint_status ")
else: 
  spark.sql("create or replace table unity_information.endpoint_status_history as select *, current_timestamp() polled_timestamp from endpoints_status_2 ")


# COMMAND ----------



# COMMAND ----------

# %sql create or replace table unity_information.endpoint_status_history as select *, current_timestamp() polled_timestamp from endpoints_status_2

# COMMAND ----------

# MAGIC %sql OPTIMIZE unity_information.endpoint_status_history 

# COMMAND ----------

# print("Time to execute: {}s".format((current_time_in_millis() - notebook_start_execution_time) / 1000))

# COMMAND ----------


