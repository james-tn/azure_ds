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
# MAGIC REFRESH TABLE  last_files_list

# COMMAND ----------

def process_df_file(df_file):
  print("at process:", df_file[1])
  #drop all the na or null rows from the dataset then save at the transformed data folder
  df_file[0].na.drop(how = 'any').toPandas().to_csv("/dbfs/mnt/transformed/"+df_file[1])
  

def process_files(files_to_load):
  df=None
  name = files_to_load[0][:-10]
  for file_to_load in files_to_load:
    print("File is: ", file_to_load)
    if df!=None:
      df = df.union(spark.read.csv("/mnt/staging/"+file_to_load, header=True))
    else:
      df = spark.read.csv("/mnt/staging/"+file_to_load, header=True)
  if len(files_to_load)>1:
    process_df_file((df, name))
  else:
    process_df_file((df, files_to_load[0]))
    
new_file_list = [item.name for item in dbutils.fs.ls("/mnt/staging")]
last_file_list = spark.sql("SELECT file_name from last_files_list").collect()
last_file_list = [row.file_name for row in last_file_list]
files_to_load = sorted(list(set(new_file_list).difference(last_file_list)))
print (new_file_list)
print(last_file_list)
print("file to load",files_to_load)
files_to_combine = []
last_file=None
for file in files_to_load:
  print("first file is: ", file)
  if "_part" in file:
    #This is a part file need to merge
    if last_file:
      if last_file[:-10]== file[:-10]:
        #this new file is still part of the last file
        files_to_combine.append(file)
      else:
        #time to perform action on combined file
        print("Files to combine: ", files_to_combine)
        process_files(files_to_combine)
        #at the end reset the combine file for the next step
        files_to_combine =[]
    else: 
      files_to_combine.append(file)
    last_file = file

  else:
    #perform ation on individual file
    process_files([file])
if (len(files_to_combine)>0):
  process_files(files_to_combine)

#upload the watermark table to mark files that are already processed

new_file_list_df = [["None",item ] for item in new_file_list]


cSchema = StructType([StructField("app_id", StringType()),StructField("file_name", StringType())])
files_df = spark.createDataFrame(new_file_list_df,schema=cSchema) 
files_df.write.mode("overwrite").saveAsTable("last_files_list")



# COMMAND ----------

print (new_file_list)
print(last_file_list)
print(files_to_load)