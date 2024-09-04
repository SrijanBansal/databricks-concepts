# Databricks notebook source
from pyspark.sql.types import StructType, StructField , IntegerType, StringType, DoubleType

schema = StructType(
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

countries_df = spark.read.csv('/FileStore/tables/countries.csv' , header ='True' , schema=schema)

# COMMAND ----------

countries_df.write.csv('/FileStore/tables/coutries_out',header=True)

# COMMAND ----------

spark.read.csv('/FileStore/tables/coutries_out' , header=True , schema=schema).display()

# COMMAND ----------

#PARTITION
countries_df.write.options(header=True).partitionBy('REGION_ID').mode('overwrite').format('csv').save('/FileStore/tables/coutries_out')

# COMMAND ----------

spark.read.csv('/FileStore/tables/coutries_out/REGION_ID=10/part-00000-tid-5941586068796890197-e27fac87-ae29-4fb9-8706-3e7a7b6981d6-35-1.c000.csv' , header=True).display()

# COMMAND ----------


