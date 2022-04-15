# Databricks notebook source
spark.read.option("header", True).option("inferSchema", True).csv("/dbstress/output").createOrReplaceTempView("dbstress_results")

# COMMAND ----------

# MAGIC %sql create database if not exists dbx_stresstesting; 

# COMMAND ----------

# MAGIC %sql create or replace table dbx_stresstesting.results as select * from dbstress_results;
