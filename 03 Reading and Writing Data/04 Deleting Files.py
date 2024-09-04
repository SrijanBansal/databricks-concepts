# Databricks notebook source
dbutils.fs.rm('/FileStore/tables/countries.txt')
dbutils.fs.rm('/FileStore/tables/countries_single_line.json')
dbutils.fs.rm('/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

dbutils.fs.rm('FileStore/tables/countries_parquet', recurse = True)

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/coutries_out', recurse = True)

# COMMAND ----------


