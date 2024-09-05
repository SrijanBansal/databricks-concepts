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

from pyspark.sql.functions import current_timestamp
countries = countries.withColumn('Timestamp', current_timestamp())

# COMMAND ----------

countries.display()

# COMMAND ----------

from pyspark.sql.functions import month
countries.select(month(countries['Timestamp'])).display()

# COMMAND ----------

from pyspark.sql.functions import year
countries.select(year(countries['Timestamp'])).display()

# COMMAND ----------

from pyspark.sql.functions import lit
countries = countries.withColumn('Date Literal',lit('27-10-2020'))

# COMMAND ----------

display(countries)

# COMMAND ----------

countries.dtypes

# COMMAND ----------


from pyspark.sql.functions import to_date
countries.withColumn('Date',to_date(countries['Date Literal'],'dd-MM-yyyy')).display()

# COMMAND ----------


