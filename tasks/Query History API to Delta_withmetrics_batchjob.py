# Databricks notebook source
dbutils.widgets.text('host_name','https://adb-2290777133481849.9.azuredatabricks.net','Host Name')

# COMMAND ----------

# DBTITLE 1,Constants
# If you want to run this notebook yourself, you need to create a Databricks personal access token,
# store it using our secrets API, and pass it in through the Spark config, such as this:
# spark.pat_token {{secrets/query_history_etl/user}}, or Azure Keyvault.

WORKSPACE_HOST = dbutils.widgets.get('host_name')
ENDPOINTS_URL = "{0}/api/2.0/sql/endpoints".format(WORKSPACE_HOST)
QUERIES_URL = "{0}/api/2.0/sql/history/queries".format(WORKSPACE_HOST)

MAX_RESULTS_PER_PAGE = 100
MAX_PAGES_PER_RUN = 500

# We will fetch all queries that were started between this number of hours ago, and now()
# Queries that are running for longer than this will not be updated.
# Can be set to a much higher number when backfilling data, for example when this Job didn't
# run for a while.
NUM_HOURS_TO_UPDATE = 24

DATABASE_NAME = "unity_information"
ENDPOINTS_TABLE_NAME = "endpoints"
QUERIES_TABLE_NAME = "endpoint_queries_with_metrics"

#Databricks secrets API
#auth_header = {"Authorization" : "Bearer " + spark.conf.get("spark.pat_token")}
#Azure KeyVault
auth_header = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "etl", key = "dbsql")}

# COMMAND ----------

# MAGIC %sql set spark.databricks.delta.schema.autoMerge.enabled = true

# COMMAND ----------

# DBTITLE 1,Imports
import requests, json
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
notebook_start_execution_time = current_time_in_millis()

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(DATABASE_NAME))

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

# endpoints_table = DeltaTable.forName(spark, "{0}.{1}".saveAsTable(DATABASE_NAME + "." + ENDPOINTS_TABLE_NAME)
# # endpoints_table.alias("endpointResults").merge(endpoints.alias("newEndpointResults"),"endpointResults.id = newEndpointResults.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                                     

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE if not exists unity_information.endpoint_queries_with_metrics (
# MAGIC   endpoint_id STRING,
# MAGIC   executed_as_user_id BIGINT,
# MAGIC   executed_as_user_name STRING,
# MAGIC   execution_end_time_ms BIGINT,
# MAGIC   is_final BOOLEAN,
# MAGIC   lookup_key STRING,
# MAGIC   plans_state STRING,
# MAGIC   query_end_time_ms BIGINT,
# MAGIC   query_id STRING,
# MAGIC   query_start_time_ms BIGINT,
# MAGIC   query_text STRING,
# MAGIC   rows_produced BIGINT,
# MAGIC   spark_ui_url STRING,
# MAGIC   status STRING,
# MAGIC   user_id BIGINT,
# MAGIC   user_name STRING,
# MAGIC   dbsql_version STRING,
# MAGIC   name STRING,
# MAGIC   compilation_time_ms BIGINT,
# MAGIC   execution_time_ms BIGINT,
# MAGIC   network_sent_bytes BIGINT,
# MAGIC   photon_total_time_ms BIGINT,
# MAGIC   pruned_bytes BIGINT,
# MAGIC   pruned_files_count BIGINT,
# MAGIC   read_bytes BIGINT,
# MAGIC   read_cache_bytes BIGINT,
# MAGIC   read_files_count BIGINT,
# MAGIC   read_partitions_count BIGINT,
# MAGIC   read_remote_bytes BIGINT,
# MAGIC   result_fetch_time_ms BIGINT,
# MAGIC   result_from_cache BOOLEAN,
# MAGIC   rows_produced_count BIGINT,
# MAGIC   rows_read_count BIGINT,
# MAGIC   spill_to_disk_bytes BIGINT,
# MAGIC   task_total_time_ms BIGINT,
# MAGIC   total_time_ms BIGINT,
# MAGIC   write_remote_bytes BIGINT)
# MAGIC 
# MAGIC   

# COMMAND ----------

query_results_agg = spark.sql(f"select * from {DATABASE_NAME}.{QUERIES_TABLE_NAME} where 1 = 2")

# COMMAND ----------

# DBTITLE 1,Fetch from Query History API
start_date = datetime.now() - timedelta(hours=NUM_HOURS_TO_UPDATE)
start_time_ms = start_date.timestamp() * 1000
end_time_ms = datetime.now().timestamp() * 1000

next_page_token = None
has_next_page = True
pages_fetched = 0

while (has_next_page and pages_fetched < MAX_PAGES_PER_RUN):
  print("Starting to fetch page " + str(pages_fetched))
  pages_fetched += 1
  if next_page_token:
    # Can not set filters after the first page
    request_parameters = {
      "max_results": MAX_RESULTS_PER_PAGE,
      "page_token": next_page_token,
      "include_metrics": "true"
    }
  else:
    request_parameters = {
      "max_results": MAX_RESULTS_PER_PAGE,
      "filter_by": {"query_start_time_range": {"start_time_ms": start_time_ms, "end_time_ms": end_time_ms}},
      "include_metrics": "true"
    }

  print ("Request parameters: " + str(request_parameters))
  
  response = requests.get(QUERIES_URL, headers=auth_header, json=request_parameters)
  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()
  
  
  if response_json["has_next_page"] == False:
    break
    
  json_data = json.dumps(response_json["res"])
  

    
  next_page_token = response_json["next_page_token"]
  has_next_page = response_json["has_next_page"]
  
#   boolean_keys_to_convert = set(get_boolean_keys(json_data))
#   for array_to_process in json_data:
#     for key in boolean_keys_to_convert:
#       array_to_process[key] = str(array_to_process[key]).lower()
    
  
  rdd_results = sc.parallelize([json_data])
  query_results = spark.read.json(rdd_results)
  
#   # For querying convience, add columns with the time in seconds instead of milliseconds
#   query_results_clean = query_results \
#     .withColumn("query_start_time", from_unixtime(query_results.metrics.query_start_time_ms / 1000)) \
#     .withColumn("query_end_time", from_unixtime(query_results.metrics.query_end_time_ms / 1000))

#   # The error_message column is not present in the REST API response when none of the queries failed.
#   # In that case we add it as an empty column, since otherwise the Delta merge would fail in schema
#   # validation
#   if "error_message" not in query_results_clean.columns:
#     query_results_clean = query_results_clean.withColumn("error_message", lit(""))
  
  query_results.createOrReplaceTempView("query_results")
  
  query_results_flat = spark.sql("select endpoint_id, executed_as_user_id, executed_as_user_name, execution_end_time_ms, is_final, lookup_key, plans_state, query_end_time_ms, query_id, query_start_time_ms, query_text, rows_produced, spark_ui_url, status, user_id, user_name, dbsql_version, name, compilation_time_ms, execution_time_ms, network_sent_bytes, photon_total_time_ms, pruned_bytes, pruned_files_count, read_bytes, read_cache_bytes, read_files_count, read_partitions_count, read_remote_bytes, result_fetch_time_ms, result_from_cache, rows_produced_count, rows_read_count, spill_to_disk_bytes, task_total_time_ms, total_time_ms, write_remote_bytes from (select *, channel_used.*, metrics.* from query_results) a")
  
  query_results_agg = query_results_agg.union(query_results_flat)
  


if not check_table_exist(db_tbl_name="{0}.{1}".format(DATABASE_NAME, QUERIES_TABLE_NAME)):
  # TODO: Probably makes sense to partition and/or Z-ORDER this table.
  query_results_agg.createOrReplaceTempView("query_results_agg")
  query_results_agg = spark.sql("select * from (select * , row_number() over (partition by query_id order by query_start_time_ms) as rn from query_results_agg qualify rn = 1) a").drop("rn")
  
  query_results_agg.write.format("delta").saveAsTable("{0}.{1}".format(DATABASE_NAME, QUERIES_TABLE_NAME)) 
else:
  # Merge this page of results into the Delta table. Existing records that match on query_id have
  # all their fields updated (needed because the status, end time, and error may change), and new
  # records are inserted.

  
  query_results_agg.createOrReplaceTempView("query_results_agg")
  query_results_agg = spark.sql("select * from (select * , row_number() over (partition by query_id order by query_start_time_ms) as rn from query_results_agg qualify rn = 1) a").drop("rn")
  
  queries_table = DeltaTable.forName(spark, "{0}.{1}".format(DATABASE_NAME, QUERIES_TABLE_NAME))
  queries_table.alias("query_Results").merge(
      query_results_agg.alias("newQueryResults"),
      "query_Results.query_id = newQueryResults.query_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

    # TODO: Add more merge conditions to make it more efficient.

# COMMAND ----------


