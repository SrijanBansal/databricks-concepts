# Databricks notebook source
df = spark.read.csv('/FileStore/tables/countries.csv', header=True)

# COMMAND ----------

df.write.mode('overwrite').parquet('/FileStore/tables/countries_parquet')

# COMMAND ----------

df_parquet = spark.read.parquet('/FileStore/tables/countries_parquet')

# COMMAND ----------

display(df_parquet)

# COMMAND ----------


