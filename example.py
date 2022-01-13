# Databricks notebook source
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import logging
#sc = SparkContext('local')
spark = SparkSession(sc)
print(type(spark))

# COMMAND ----------

#Job 1
data = spark.range(10, 200)
data.write.mode("overwrite").format("delta").save('/databricks/driver')
spark.read.format("delta").load('/databricks/driver').count()

# COMMAND ----------

#Job 2
spark.range(100).write.mode("overwrite").format("delta").option("overwriteSchema","true").save('/databricks/driver')
spark.read.format("delta").load('/databricks/driver').count()

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/databricks/driver')
fullHistoryDF = deltaTable.history()    # get the full history of the table

lastOperationDF = deltaTable.history(1) # get the last operation


latestHistory = deltaTable.history()
latestHistory.select("version","timestamp","operation","operationParameters").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --Select * from corporateanalyticsedw.bodfcf;32
# MAGIC 
# MAGIC --Select count(*) from corporateanalyticsedw.bodfcf;
# MAGIC 
# MAGIC --describe history corporateanalyticsedw.unbillnacy;
# MAGIC 
# MAGIC --Select * from corporateanalyticsedw.unbillnacy VERSION AS OF 20 
# MAGIC insert into corporateanalyticsedw.ABC 
# MAGIC RESTORE TABLE corporateanalyticsedw.unbillnacy to VERSION AS OF 21;
# MAGIC --TIMESTAMP AS OF <timestamp_expression> 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history salesforce.account;
# MAGIC 
# MAGIC --Select count(*) from corporateanalyticsedw.unbillnacy version as of 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from corporateanalyticsedw.time where date_add(date_trunc('week', current_date), -1 )  = DateName

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql("select date_format(current_date,'MM-01-yyyy') as Today").show()
# MAGIC #print (Todays_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view Today_Date as
# MAGIC Select date_format(current_date,'yyyy-MM-01') as MonthStart
# MAGIC       , lpad(Year(current_date),4,0) as Year
# MAGIC       , lpad(Month(current_date),2,'0') as Month
# MAGIC       , lpad(Month(current_date)-1,2,'0') as LastMonth

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Today_Date

# COMMAND ----------

# MAGIC %python
# MAGIC Year = spark.sql("Select Year from Today_Date")
# MAGIC Year.show()