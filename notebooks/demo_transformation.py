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


# COMMAND ----------

# #This is the configuration in hadoop to enable direct access to datalake store
# tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
# client_id = 'af883abf-89dd-4889-bdb3-1ee84f68465e'
# client_secret = 'qId6BcZ6z03/Z5W9lSbuLMjPvfTF4yBpVAxrBoJHVBE='
# spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
# spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.client.id", client_id)
# spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.credential", client_secret)
# spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")




# COMMAND ----------

#This is the configuration in hadoop to enable direct access to datalake store
tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
client_id = 'af883abf-89dd-4889-bdb3-1ee84f68465e'
client_secret = 'qId6BcZ6z03/Z5W9lSbuLMjPvfTF4yBpVAxrBoJHVBE='
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", client_id)
spark.conf.set("dfs.adls.oauth2.credential", client_secret)
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")




# COMMAND ----------

# #This option is to create a mount point in DBFS to map to a location in datalake store. Now for some reason, this is much faster for reading than direct access

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": client_id,
           "dfs.adls.oauth2.credential": client_secret,
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token"}
dbutils.fs.mount(
  source = "adl://adlstore01.azuredatalakestore.net/demo_data/",
  mount_point = "/mnt/demo/csv",
  extra_configs = configs)

# the mount_point is now the alias for the remote data

# COMMAND ----------

#only use this to unmount the mount point
dbutils.fs.unmount("/mnt/demo/csv")

# COMMAND ----------

#With mount point, it's easy to read from datalake store
df = spark.read.csv("/mnt/demo/csv/BostonWeather.csv", header=True)


df.show()

# COMMAND ----------

#This is the original dataset loaded from raw file. It may contain data from many event types and not all of them are compatible together
# Option 1: direct access to ADL
df1 = spark.read.csv("adl://adlstore01.azuredatalakestore.net/demo_data/201501-hubway-tripdata.csv", header=True)
df2 = spark.read.csv("adl://adlstore01.azuredatalakestore.net/demo_data/201504-hubway-tripdata.csv", header=True)
df = df1.union(df2)
#Method 1: writing result data using distributed file system supported in Spark using mount point as alias. Data will be written to multiple partition within the folder
df.write.mode("overwrite").save("/mnt/demo/csv/20150401-spark-tripdata.csv",format="com.databricks.spark.csv")
#Method 2: writing to a table format
df.select("tripduration", "starttime", "stoptime").write.mode("overwrite").saveAsTable("20150401tripdata")
#Method 3: writing using client style csv file, preserving single client file name and format. This is not recommended when data file is larger than the memory of the driver node because first it collects all data to a driver node then save to the mount point.
df.toPandas().to_csv("/dbfs/mnt/demo/csv/20150401-consol-tripdata.csv")

# COMMAND ----------


#this snipset demonstrates listing files in a directory. This can be used to implement incremental loading by comparing the files at a staging directory against a watermark table (can be a spark table or SQL table), if there're differences then we process the delta
file_list = dbutils.fs.ls("/mnt/demo/csv")
item = file_list[0]
print(item.name)

test_local_load = pd.read_csv("/dbfs/mnt/demo/csv/201510-hubway-tripdata.csv")


# COMMAND ----------

#This demonstrate using R to work directly with csv data on server. Data scientists can use this workbench to develop R directly on server data
%r
r <-read.csv("/dbfs/mnt/demo/csv/201510-hubway-tripdata.csv")
head(r)

# COMMAND ----------

# MAGIC %sql select * from 20150401tripdata

# COMMAND ----------

df3 = spark.sql("SELECT * FROM csv.`adl://adlstore01.azuredatalakestore.net/demo_data/201504-hubway-tripdata.csv` where _c9>41")
df3.show()

# COMMAND ----------

df.createOrReplaceTempView( "weather" )
sqlDF = spark.sql("SELECT REPORTTPYE, avg( HOURLYDRYBULBTEMPF ) FROM weather GROUP BY REPORTTPYE having REPORTTPYE <>'SOD'")
sqlDF.write.mode("overwrite").save("adl://adlstore01.azuredatalakestore.net/demo_data/test.csv",format="csv")
sqlDF.write.saveAsTable("test_tbl")

# COMMAND ----------

sqlDF.show()

# COMMAND ----------

#The example below is for one type of ET value.
dl_selection = df.where(df.REPORTTPYE=='FM-12')
dl_selection.show()

# COMMAND ----------

#This schema should be loaded from a DB. This hardcoded value is an example
download_selection_schema = {"Is Closed By User":"default","Customer Resume":"default","Download Speed":"0","Download Result":"default","Total Download Time":"0", "OSSWB":"D2X75","Error Code":"default","OS Image Size":"0","OS Code":"default","Chunking Information":"0"}
dl_selection_rdd_1 = dl_selection_rdd.map(lambda row: process_val_row(row, download_selection_schema))

dl_selection_rdd_clean= dl_selection_rdd_1.filter(lambda row: "invalid_schema" not in row)


# COMMAND ----------

#show samples invalid& unregistered records
dl_selection_rdd_1.filter(lambda row: "invalid_schema"  in row).take(10)

# COMMAND ----------


dl_selection_df = spark.createDataFrame(dl_selection_rdd_clean,samplingRatio=0.1)

dl_selection_df.show()

# COMMAND ----------

dl_selection.write.mode("overwrite").format("com.databricks.spark.avro").saveAsTable("fm_12_tbl", path="adl://adlstore01.azuredatalakestore.net/demo_data/tables")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table new_table as (select * from fm_12_tbl where id=)