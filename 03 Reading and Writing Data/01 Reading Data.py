# Databricks notebook source
countries_df = spark.read.csv('/FileStore/tables/countries.csv' , header ='True' , inferSchema= True)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.printSchema()

# COMMAND ----------

countries_df.schema


# COMMAND ----------

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

#schema
countries_df = spark.read.csv('/FileStore/tables/countries.csv' , header ='True' , schema=schema)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.printSchema()

# COMMAND ----------

#Alternative method
spark.read.options(header=True).schema(schema).csv("dbfs:/FileStore/tables/countries.csv").display()

# COMMAND ----------

countries_single_line_df = spark.read.schema(schema).json('/FileStore/tables/countries_single_line.json')

# COMMAND ----------

display(countries_single_line_df)

# COMMAND ----------

countries_single_line_df = spark.read.format('json').schema(schema).load('/FileStore/tables/countries_single_line.json')

# COMMAND ----------

countries_multi_line_df = spark.read.options(multiLine=True).schema(schema).json('/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

display(countries_multi_line_df)

# COMMAND ----------

countries_txt = spark.read.csv('/FileStore/tables/countries.txt' , header= True , sep = '\t')

# COMMAND ----------

display(countries_txt)

# COMMAND ----------

countries_txt = spark.read.options(header = True , sep='\t').csv('/FileStore/tables/countries.txt')

# COMMAND ----------

display(countries_txt)

# COMMAND ----------


