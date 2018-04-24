# Databricks notebook source

import pyspark
import random
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import Row 
import ast
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE  last_transform_files

# COMMAND ----------


def process_files(files_to_load):
  df=None
  for file_to_load in files_to_load:
    print("File is: ", file_to_load)
    if df!=None:
      df = df.union(spark.read.csv("/mnt/transformed/"+file_to_load, header=True))
    else:
      df = spark.read.csv("/mnt/transformed/"+file_to_load, header=True)
  df = df.select("tripduration", "starttime", "stoptime").filter(df.tripduration>1000)
  df.write.mode("append").saveAsTable("demo_transform_trip_data")
new_file_list = [item.name for item in dbutils.fs.ls("/mnt/transformed")]
last_file_list = spark.sql("SELECT file_name from last_transform_files where app_id = 'transform'").collect()
last_file_list = [row.file_name for row in last_file_list]
files_to_load = sorted(list(set(new_file_list).difference(last_file_list)))
print (new_file_list)
print(last_file_list)
print("file to load",files_to_load)
files_to_combine = []
last_file=None
for file in files_to_load:
  print("first file is: ", file)
  if "_ready" in file:
    files_to_combine.append(file)

if (len(files_to_combine)>0):
  process_files(files_to_combine)

#upload the watermark table to mark files that are already processed

new_file_list_df = [["transform",item ] for item in new_file_list]


cSchema = StructType([StructField("app_id", StringType()),StructField("file_name", StringType())])
files_df = spark.createDataFrame(new_file_list_df,schema=cSchema) 
files_df.write.mode("overwrite").saveAsTable("last_transform_files")



# COMMAND ----------

