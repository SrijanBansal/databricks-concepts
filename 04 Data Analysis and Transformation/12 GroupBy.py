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

countries.groupBy('region_id').sum('population').sort('region_id').display()

# COMMAND ----------

countries.groupBy('region_id').avg('population').sort('region_id').display()

# COMMAND ----------

countries.groupBy('region_id').sum('population','area_km2').sort('region_id').display()

# COMMAND ----------

from pyspark.sql.functions import *
countries.groupBy('region_id','sub_region_id').agg(sum('population'),max('population')).sort('region_id')\
    .withColumnRenamed('sum(population)','Total_Population')\
    .withColumnRenamed('max(population)','Max_Population').display()

# COMMAND ----------

countries.groupBy('region_id','sub_region_id').agg(min('population').alias('min_pop'),max('population').alias('max_pop')).sort('region_id').display()

# COMMAND ----------


