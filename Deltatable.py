# Databricks notebook source
# MAGIC %run /CorporateAnalytics/EDW/Common_Params

# COMMAND ----------

#For SupportWebinar
import os
from datetime import datetime, date, timedelta
tempDate_SupportWebinar= datetime.today()-timedelta(365)
tempDate_CloudBatchMisses= datetime.today()-timedelta(365)
path = '/dbfs/FileStore/shared_uploads/nivedhitha.krishnan@jda.com'
fdpaths = [path+"/"+fd for fd in os.listdir(path)]
for fdpath in fdpaths:
  if(fdpath.find("SupportWebinar")>0):
    statinfo = os.stat(fdpath)
    create_date = datetime.fromtimestamp(statinfo.st_ctime)
    #modified_date = datetime.fromtimestamp(statinfo.st_mtime)
    if create_date > tempDate_SupportWebinar:
      tempDate_SupportWebinar = create_date
      tempFile_SupportWebinar=fdpath
  if(fdpath.find("CloudBatchMisses")>0):
    statinfo = os.stat(fdpath)
    create_date = datetime.fromtimestamp(statinfo.st_ctime)
    #modified_date = datetime.fromtimestamp(statinfo.st_mtime)
    if create_date > tempDate_CloudBatchMisses:
      tempDate_CloudBatchMisses = create_date
      tempFile_CloudBatchMisses=fdpath
tempFile_SupportWebinar = tempFile_SupportWebinar[5:]
tempFile_SupportWebinar
tempFile_CloudBatchMisses = tempFile_CloudBatchMisses[5:]
tempFile_CloudBatchMisses

Webinar = spark.read.format("csv").option("header","true").load(tempFile_SupportWebinar)
Webinar.createOrReplaceTempView("Webinar")

CloudBatchMisses = spark.read.format("csv").option("header","true").load(tempFile_CloudBatchMisses)
CloudBatchMisses.createOrReplaceTempView("CloudBatchMisses")