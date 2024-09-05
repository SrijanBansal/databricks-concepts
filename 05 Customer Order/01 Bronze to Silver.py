# Databricks notebook source
from pyspark.sql.types import StructType, StructField , IntegerType, StringType , DoubleType

# COMMAND ----------

orders_path = '/FileStore/tables/Bronze/orders.csv'
orders_schema =StructType ([
                                    StructField('ORDER_ID', IntegerType(), False),
                                    StructField('ORDER_DATETIME', StringType(), False),
                                    StructField('CUSTOMER_ID', IntegerType(), False),
                                    StructField('ORDER_STATUS', StringType(), False),
                                    StructField('STORE_ID', IntegerType(), False)
                                    ])
orders_df = spark.read.csv(path = orders_path , schema = orders_schema , header = True)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

orders_df_select  = orders_df.select('ORDER_ID',
                to_timestamp(orders_df['ORDER_DATETIME'],'dd-MMM-yy kk.mm.ss.SS').alias('ORDER_TIMESTAMP'),
                'CUSTOMER_ID','ORDER_STATUS','STORE_ID')

# COMMAND ----------

display(orders_df_select)

# COMMAND ----------

orders_df_filtered = orders_df_select.filter(orders_df['ORDER_STATUS']=='COMPLETE')

# COMMAND ----------

stores_path = '/FileStore/tables/Bronze/stores.csv'
stores_schema = StructType([
                              StructField('STORE_ID', IntegerType(), False)
                            , StructField('STORE_NAME', StringType(), False)
                            , StructField('WEB_ADDRESS', StringType(), False)
                            , StructField('LATITUDE', DoubleType(), False)
                            , StructField('LONGITUDE', DoubleType(), False)
                            ])

# COMMAND ----------

stores_df = spark.read.csv(path =stores_path, schema=stores_schema, header = True)

# COMMAND ----------

display(stores_df)

# COMMAND ----------

orders_df_join = orders_df_filtered.join(stores_df, orders_df_filtered['STORE_ID'] ==stores_df['STORE_ID'] , 'inner').select('ORDER_ID','ORDER_TIMESTAMP','CUSTOMER_ID','STORE_NAME')

# COMMAND ----------

customers_path = '/FileStore/tables/Bronze/customers.csv'
customers_schema =StructType([
                        StructField('CUSTOMER_ID', IntegerType(), False),
                         StructField('FULL_NAME', StringType(), False), 
                         StructField('EMAIL_ADDRESS', StringType(), False)
                         ])

customers_df = spark.read.csv(path = customers_path, schema = customers_schema , header = True)                      

# COMMAND ----------

display(customers_df)

# COMMAND ----------

order_items_path = '/FileStore/tables/Bronze/order_items.csv'
order_items_schema =StructType([
                    StructField('ORDER_ID', IntegerType(), False),
                     StructField('LINE_ITEM_ID', IntegerType(), False),
                      StructField('PRODUCT_ID', IntegerType(), False),
                       StructField('UNIT_PRICE', DoubleType(), False),
                        StructField('QUANTITY', IntegerType(), False
                        )])

order_items_df = spark.read.csv(path=order_items_path, schema = order_items_schema, header = True)

# COMMAND ----------

display(order_items_df)

# COMMAND ----------

order_items_df = order_items_df.drop('LINE_ITEM_ID')

# COMMAND ----------

products_path = '/FileStore/tables/Bronze/products.csv'
products_schema = StructType([
                        StructField('PRODUCT_ID', IntegerType(), False),
                        StructField('PRODUCT_NAME', StringType(), False), 
                        StructField('UNIT_PRICE', DoubleType(), False)
                        ])

products_df = spark.read.csv( path=products_path, schema = products_schema, header =True )

# COMMAND ----------

display(products_df)

# COMMAND ----------

orders_df_join.write.mode('overwrite').format('parquet').save('/FileStore/tables/Silver/orders.parquet')
customers_df.write.mode('overwrite').format('parquet').save('/FileStore/tables/Silver/customers.parquet')
order_items_df.write.mode('overwrite').format('parquet').save('/FileStore/tables/Silver/order_items.parquet')
products_df.write.mode('overwrite').format('parquet').save('/FileStore/tables/Silver/products.parquet')
