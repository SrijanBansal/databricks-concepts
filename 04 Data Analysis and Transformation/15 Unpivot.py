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

countries1 =countries.join(countries_regions_df , countries['region_id']== countries_regions_df['ID'], 'inner').select(countries_regions_df['NAME'].alias('Region_name'),countries['NAME'].alias('Country_name'),countries['POPULATION']).sort(countries['POPULATION'].desc())

# COMMAND ----------

countries2 = countries1.groupBy('Country_name').pivot('Region_name').sum('POPULATION')

# COMMAND ----------

display(countries2)

# COMMAND ----------

from pyspark.sql.functions import expr
countries2.select("Country_name", expr("stack(5 , 'Asia', Asia, 'Europe', Europe, 'Africa', Africa, 'America', America, 'Oceania', Oceania) as (Region_name, POPULATION)")).display()

# COMMAND ----------


