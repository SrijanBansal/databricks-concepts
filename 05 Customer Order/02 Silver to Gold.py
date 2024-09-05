# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### ORDER_DETAILS

# COMMAND ----------

orders = spark.read.parquet('/FileStore/tables/Silver/orders.parquet')
order_items = spark.read.parquet('/FileStore/tables/Silver/order_items.parquet')
customers = spark.read.parquet('/FileStore/tables/Silver/customers.parquet')
products = spark.read.parquet('/FileStore/tables/Silver/products.parquet')

# COMMAND ----------

orders_select = orders.select('ORDER_ID', to_date(orders['ORDER_TIMESTAMP'],'yyy-MM-dd').alias('ORDER_DATE'),
              'CUSTOMER_ID', 'STORE_NAME')

# COMMAND ----------

display(orders_select)

# COMMAND ----------

order_items.columns

# COMMAND ----------

orders_details = orders_select.join(order_items , orders_select['ORDER_ID'] == order_items['ORDER_ID'] , 'left' ).select(orders_select['ORDER_ID'] , orders_select['ORDER_DATE'] , orders_select['CUSTOMER_ID'], orders_select['STORE_NAME'] ,order_items['UNIT_PRICE'], order_items['QUANTITY'] )

# COMMAND ----------

display(orders_details)

# COMMAND ----------

orders_details = orders_details.withColumn('TOTAL_SALES_AMOUNT', expr('UNIT_PRICE * QUANTITY'))

# COMMAND ----------

display(orders_details)

# COMMAND ----------

orders_details = orders_details.groupBy(orders_details['ORDER_ID'] , orders_details['ORDER_DATE'] , orders_details['CUSTOMER_ID'], orders_details['STORE_NAME']).sum('TOTAL_SALES_AMOUNT').withColumnRenamed('sum(TOTAL_SALES_AMOUNT)','TOTAL_ORDER_AMOUNT')

# COMMAND ----------

orders_details = orders_details.withColumn('TOTAL_ORDER_AMOUNT' , round('TOTAL_ORDER_AMOUNT',2))

# COMMAND ----------

display(orders_details)

# COMMAND ----------

orders_details.write.mode("overwrite").save("/FileStore/tables/Gold/orders_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MONTHLY_DETAILS

# COMMAND ----------

monthly_sales = orders_details.withColumn( 'MONTH_YEAR' , date_format('ORDER_DATE' , 'yyyy-MM'))

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

monthly_sales = monthly_sales.groupBy('MONTH_YEAR').sum('TOTAL_ORDER_AMOUNT').\
    withColumn('TOTAL_SALES' , round('sum(TOTAL_ORDER_AMOUNT)' , 2)).sort(monthly_sales['MONTH_YEAR'].desc()).select('MONTH_YEAR','TOTAL_SALES')

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

monthly_sales.write.mode("overwrite").save("/FileStore/tables/Gold/monthly_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### STORE_MONTHLY_SALES
# MAGIC

# COMMAND ----------

stores_monthly_sales = orders_details.withColumn( 'MONTH_YEAR' , date_format('ORDER_DATE' , 'yyyy-MM'))

# COMMAND ----------

stores_monthly_sales = stores_monthly_sales.groupBy('MONTH_YEAR' ,'STORE_NAME').sum('TOTAL_ORDER_AMOUNT').\
    withColumn('TOTAL_SALES' , round('sum(TOTAL_ORDER_AMOUNT)',2)).sort(stores_monthly_sales['MONTH_YEAR'].desc()).select('MONTH_YEAR' ,'STORE_NAME','TOTAL_SALES')

# COMMAND ----------

display(stores_monthly_sales)

# COMMAND ----------

stores_monthly_sales.write.mode("overwrite").save("/FileStore/tables/Gold/stores_monthly_sales")

# COMMAND ----------


