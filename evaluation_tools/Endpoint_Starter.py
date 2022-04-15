# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Endpoint Starter
# MAGIC 
# MAGIC Starts Endpoint, waits for it to be running, looping every 10 seconds, then exits when running

# COMMAND ----------

dbutils.widgets.text('host_name','adb-2290777133481849.9.azuredatabricks.net','Host Name')
dbutils.widgets.text('endpoint_id','d020197a762f4381','Endpoint ID')


# COMMAND ----------

endpoint_id = dbutils.widgets.get('endpoint_id')
WORKSPACE_HOST = dbutils.widgets.get('host_name')

# COMMAND ----------

import requests, json, time

# COMMAND ----------

# WORKSPACE_HOST = 'https://e2-demo-west.cloud.databricks.com'
ENDPOINTS_URL = f"{WORKSPACE_HOST}/api/2.0/sql/endpoints/{endpoint_id}/start"
auth_header = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "etl", key = "dbsql")}

# COMMAND ----------

response = requests.post(ENDPOINTS_URL, headers=auth_header)

if response.status_code != 200:
  raise Exception(response.text)
response_json = response.json()

# endpoints_json = response_json["endpoints"]


# COMMAND ----------

ENDPOINTS_URL = f"{WORKSPACE_HOST}/api/2.0/sql/endpoints/{endpoint_id}"
auth_header = {"Authorization" : "Bearer " + dbutils.secrets.get(scope = "etl", key = "dbsql")}

# COMMAND ----------

endpoint_state=''

while endpoint_state != 'RUNNING':
  response = requests.get(ENDPOINTS_URL, headers=auth_header)
  if response.status_code != 200:
    raise Exception(response.text)
  response_json = response.json()
  endpoint_state = response_json["state"]
  print(endpoint_state)
  print("sleeping for 10 Seconds... ")
  time.sleep(10)


# COMMAND ----------


