# Databricks notebook source
dbutils.widgets.dropdown('clean_past_runs','false',['true','false'])
past_runs_flag = dbutils.widgets.get('clean_past_runs')

# COMMAND ----------

if past_runs_flag:
  dbutils.fs.rm('/dbfs/dbstress/output/*')
