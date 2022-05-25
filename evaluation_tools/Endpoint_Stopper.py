# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Endpoint Stopper
# MAGIC 
# MAGIC Starts Endpoint, waits for it to be running, looping every 10 seconds, then exits when running

# COMMAND ----------

# {
#   "host_name": "e2-demo-west.cloud.databricks.com/",
#   "endpoint_id": "b6019c8f57bcdda9"
# }

# COMMAND ----------

dbutils.widgets.text('host_name','adb-2290777133481849.9.azuredatabricks.net','Host Name')
dbutils.widgets.text('endpoint_id','d020197a762f4381','Endpoint ID')


# COMMAND ----------

endpoint_id = dbutils.widgets.get('endpoint_id')

# COMMAND ----------

import requests, json, time

# COMMAND ----------

WORKSPACE_HOST = dbutils.widgets.get('host_name')
ENDPOINTS_URL = f"{WORKSPACE_HOST}/api/2.0/sql/endpoints/{endpoint_id}/stop"
auth_header = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "etl", key = "dbsql")}

# COMMAND ----------

response = requests.post(ENDPOINTS_URL, headers=auth_header)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()

# endpoints_json = response_json["endpoints"]


# COMMAND ----------


