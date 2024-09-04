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

display(countries)

# COMMAND ----------

#case sensitvity doesnt matter

countries.select('name','capital','population').display()

# COMMAND ----------

#Better to use when we want to add methods with columns
#case sensitvity doesnt matter

countries.select(countries['name'],countries['capital'],countries['population']).display()

# COMMAND ----------

#This method is case sensitive

countries.select(countries.NAME , countries.CAPITAL , countries.POPULATION).display()

# COMMAND ----------

from pyspark.sql.functions import col

countries.select(col('name'),col('capital'),col('population')).display()

# COMMAND ----------

#All the methods for selecting columns

countries.select('name','capital','population').display()
countries.select(countries['name'],countries['capital'],countries['population']).display()
countries.select(countries.NAME , countries.CAPITAL , countries.POPULATION).display()

from pyspark.sql.functions import col
countries.select(col('name'),col('capital'),col('population')).display()



# COMMAND ----------

# alias method

countries.select(countries['name'].alias('Country_Name'),countries['capital'].alias('Capital_city'),countries['population'].alias('population')).display()

# COMMAND ----------

# withColumnRenamed Method

countries.select('name','capital','population').withColumnRenamed('name','Country_Name').withColumnRenamed('capital','Capital_city').display()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField , IntegerType, StringType
countries_regions_path = '/FileStore/tables/country_regions.csv'
countries_regions_schema = StructType(
    [StructField('ID', IntegerType(), False),
    StructField('NAME', StringType(), False)]
    )

countries_regions_df = spark.read.csv(path = countries_regions_path, schema=countries_regions_schema, header=True)

# COMMAND ----------

display(countries_regions_df)

# COMMAND ----------

countries_regions_df.select('ID','NAME').withColumnRenamed('NAME', 'Continent').display()

from pyspark.sql.functions import col
countries_regions_df.select(col('ID'),col('NAME').alias('Continent')).display()

# COMMAND ----------


