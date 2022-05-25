# Databricks notebook source
# MAGIC %pip install sqlparse

# COMMAND ----------


# {
#   "parallel_conn": "5",
#   "host_name": "adb-2290777133481849.9.azuredatabricks.net",
#   "http_path": "/sql/1.0/endpoints/460f103e1bb95914",
#   "database_name": "tpcds_sf100_delta_nopartitions",
#   "re_peats": "40"
# }

# {"database_name":"tpcds_sf1000_delta_nopartitions","host_name":"e2-demo-west.cloud.databricks.com","http_path":"/sql/1.0/endpoints/b6019c8f57bcdda9","parallel_conn":"2","re_peats":"2"}

# COMMAND ----------

dbutils.widgets.text('database_name','tpcds_sf1000_delta_nopartitions','Database Name')
dbutils.widgets.text('host_name','e2-demo-west.cloud.databricks.com','Host Name')
dbutils.widgets.text('http_path','/sql/1.0/endpoints/b6019c8f57bcdda9','http path')
dbutils.widgets.text('parallel_conn','2','Parallel Connections')
dbutils.widgets.text('re_peats','2','Query Repeats')
database = dbutils.widgets.get('database_name')
hostname = dbutils.widgets.get('host_name')
httpPath = dbutils.widgets.get('http_path')
parallel_conn = dbutils.widgets.get('parallel_conn')
re_peats = dbutils.widgets.get('re_peats')

# COMMAND ----------

# MAGIC %md
# MAGIC # Dependencies
# MAGIC install custom dbstress jar from https://github.com/PavanDendi/dbstress/tree/databricks
# MAGIC install sql-metadata from pip:
# MAGIC `%pip install sql-metadata`
# MAGIC 
# MAGIC Compiled binary can be found in the github Releases [here](https://github.com/PavanDendi/dbstress/releases/download/0.0.0/dbstress-assembly-0.0.0-SNAPSHOT.jar), or compile from source using `sbt assembly`
# MAGIC 
# MAGIC `yaml_str()` expects a single query at a time

# COMMAND ----------

# dbutils.secrets.get(scope = "etl", key = "dbstress")

# COMMAND ----------

# DBTITLE 1,Config variables
# Personal Access Token created by user
# https://docs.microsoft.com/en-us/azure/databricks/sql/user/security/personal-access-tokens
token = dbutils.secrets.get(scope = "etl", key = "dbsql")

# database to use when querying SQL endpoint
#database = "tpcds_sf10_delta_nopartitions"

# Connection string from SQL endpoint
# Replace hostname and httpPath with user's specific connection details
# Remove "LogLevel=4" to stop debug output, and increase performance
# Shared Endpoint - Photon
#hostname = "e2-demo-west.cloud.databricks.com"

port = "443"
#httpPath = "/sql/1.0/endpoints/d99d20fe77965310"
jdbc = f"jdbc:spark://{hostname}:443/{database};transportMode=http;ssl=1;AuthMech=3;httpPath={httpPath};AuthMech=3;UID=token;PWD={token};UseNativeQuery=1;IgnoreTransactions=1;FastConnection=1;use_cached_result=false"

# Driver class to use; change only if using a different jdbc driver
driver_class = "com.simba.spark.jdbc.Driver"

# path and filename for generated yaml
yaml_path = "/dbfs/dbstress/configs/test_config_2.yaml"

# path for dbstress result outputs
result_path = "/dbfs/dbstress/output/"

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir /dbfs/dbstress/
# MAGIC mkdir /dbfs/dbstress/output
# MAGIC mkdir /dbfs/dbstress/configs/

# COMMAND ----------

# MAGIC %md
# MAGIC # Create yaml config file for dbstress

# COMMAND ----------

# DBTITLE 1,Retrieving headers and queries from SQL file with multiple queries
from pathlib import Path
import os 

def read_sql_folder(path=None):
    path = Path(path)
    sql_paths = sorted(list(path.glob('./*.sql')))
    query_tuples = []
    for file in sql_paths:
        with open(file, "r") as f:
            query_tuples.append((Path(file).stem, f.read()))
            print(f.name)
    return query_tuples

query_tuples = read_sql_folder("/Workspace/Repos/franco.patano@databricks.com/dUI/queries/tpcds_2.13")

# COMMAND ----------

# DBTITLE 1,yaml generator function
def yaml_str(unit_name, query, uri, driver_class=None, parallel=1, repeats=1, connection_timeout=3000):
  out = "---\n"
  out += f"unit_name: {unit_name}\n"
  out += f'query: "/* {unit_name}  @@gen_query_id@@ */\n{query}"\n'
  out += f'uri: "{uri}"\n'
  out += f"driver_class: {driver_class}\n"
  out += "username: username\npassword: password\n"
  out += f"parallel_connections: {parallel}\n"
  out += f"repeats: {repeats}\n"
  out += f"connection_timeout: {connection_timeout}\n"
  return out

# COMMAND ----------

query_exec = []
for t in query_tuples:
  query_exec  += [t[0], t[1]]

# COMMAND ----------

import sqlparse

# Specify the queries we wish to run concurrently

#query_exec =[query_tuples[0], query_tuples[1]]
#generalized = [(name, sqlparse.format(q, strip_comments=True).strip()) for (name, q) in query_exec]

generalized = []
for t in query_tuples:
  generalized.append([t[0], sqlparse.format(t[1], strip_comments=True).strip()])
  #print(t[0])

yaml_out = ""
for t in generalized:
  yaml_out += yaml_str(t[0], t[1], jdbc, driver_class, parallel_conn, re_peats)
 
print(yaml_out)

with open(yaml_path, "w") as f:
  f.write(yaml_out)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run dbstress with generated yaml

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql.DriverManager
# MAGIC // import eu.semberal.dbstress._
# MAGIC 
# MAGIC DriverManager.registerDriver(new com.simba.spark.jdbc.Driver())

# COMMAND ----------

def toJStringArray(arr):
    jarr = sc._gateway.new_array(sc._jvm.java.lang.String, len(arr))
    for i in range(len(arr)):
        jarr[i] = arr[i]
    return jarr
  
#sc._jvm.py4j.GatewayServer.turnLoggingOn()

args = toJStringArray(["-c",yaml_path,"-o",result_path])

sc._jvm.eu.semberal.dbstress.Main.main(args)

# COMMAND ----------


