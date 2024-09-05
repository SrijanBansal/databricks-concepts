# Databricks notebook source
from pyspark.sql.types import StructType, StructField , IntegerType, StringType, DoubleType

countries_path = '/FileStore/tables/countries.csv'

countries_schema = StructType(
            [StructField('COUNTRY_ID', IntegerType(), False),
            StructField('NAME', StringType(), False),
            StructField('NATIONALITY', StringType(), False), 
            StructField('COUNTRY_CODE', StringType(), False),
            StructField('ISO_ALPHA2', StringType(), False),
            StructField('CAPITAL', StringType(), False),
            StructField('POPULATION', DoubleType(), False),
            StructField('AREA_KM2', IntegerType(), False),
            StructField('REGION_ID', IntegerType(), True), 
            StructField('SUB_REGION_ID', IntegerType(), True),
            StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
            StructField('ORGANIZATION_REGION_ID', IntegerType(), True)
            ])

# COMMAND ----------

countries = spark.read.csv(path =countries_path , schema = countries_schema , header =True)

# COMMAND ----------

countries.select('NAME').filter(countries['POPULATION']>1000000000).display()

# COMMAND ----------

countries.filter("region_id == 10 and population == 0
                 " ).display()

# COMMAND ----------

from pyspark.sql.functions import length

countries.filter(   (length(countries['NAME'])>15) & (countries['region_id'] != 10)  ).display()


# COMMAND ----------


