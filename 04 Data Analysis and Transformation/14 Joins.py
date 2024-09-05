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

from pyspark.sql.types import StructType, StructField , IntegerType, StringType
countries_regions_path = '/FileStore/tables/country_regions.csv'
countries_regions_schema = StructType(
    [StructField('ID', IntegerType(), False),
    StructField('NAME', StringType(), False)]
    )

countries_regions_df = spark.read.csv(path = countries_regions_path, schema=countries_regions_schema, header=True)

# COMMAND ----------

display(countries)

# COMMAND ----------

display(countries_regions_df)

# COMMAND ----------

countries.join(countries_regions_df , countries['region_id']== countries_regions_df['ID'], 'inner').display()

# COMMAND ----------

countries.join(countries_regions_df , countries['region_id']== countries_regions_df['ID'], 'inner').select(countries['NAME'],countries_regions_df['NAME'],countries['region_id']).display()

# COMMAND ----------

countries.join(countries_regions_df , countries['region_id']== countries_regions_df['ID'], 'inner').select(countries_regions_df['NAME'].alias('Region_name'),countries['NAME'].alias('Country_name'),countries['POPULATION']).sort(countries['POPULATION'].desc()).display()
