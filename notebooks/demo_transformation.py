# Databricks notebook source

import pyspark
import random
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import Row 
import ast
from pyspark.sql.functions import *



# COMMAND ----------

#This function extract EV column and construct new columns out of key-value pairs
#Assuming we pass on a schema as a dictionary in format {key1:default_value,key2:default_value...}
def process_val_row(row, schema):
    #step 0: retrieve complex EV as a dict
    val = row.asDict()
    ev_dict = ast.literal_eval(val.get('EV'))
    #step 1: validation to make sure the actual schema is registered, if unregistered, return invalid status together with data
    if (not set(ev_dict.keys()).issubset(set(schema.keys()))): 
      return {"invalid_schema":"",**ev_dict, **val}
    #step 2: process valid records    
    updated_new_dict = {reg_key: ev_dict.get(reg_key,schema[reg_key]) for reg_key in schema.keys()}
    for key in schema.keys():
        if " " in key:
          updated_new_dict[key.replace(" ", "_")] = updated_new_dict.pop(key)
    del val['EV']
    combined_dict = {**updated_new_dict, **val }

    return Row(**combined_dict)

# COMMAND ----------

#This is the configuration in hadoop to enable direct access to datalake store
tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
client_id = 'af883abf-89dd-4889-bdb3-1ee84f68465e'
client_secret = 'qId6BcZ6z03/Z5W9lSbuLMjPvfTF4yBpVAxrBoJHVBE='
spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.client.id", client_id)
spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.credential", client_secret)
spark.sparkContext._jsc.hadoopConfiguration().set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/"+tenant_id+"/oauth2/token")




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

#This option is to create a mount point in DBFS to map to a location in datalake store. Now for some reason, this is much faster for reading than direct access

configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": 'af883abf-89dd-4889-bdb3-1ee84f68465e',
           "dfs.adls.oauth2.credential": 'qId6BcZ6z03/Z5W9lSbuLMjPvfTF4yBpVAxrBoJHVBE=',
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}
dbutils.fs.mount(
  source = "adl://adlstore01.azuredatalakestore.net/demo_data/",
  mount_point = "/mnt/demo/csv",
  extra_configs = configs)



# COMMAND ----------

dbutils.fs.unmount("/mnt/demo/csv")

# COMMAND ----------

df = spark.read.csv("/mnt/demo/csv/BostonWeather.csv", header=True)


df.show()

# COMMAND ----------



# COMMAND ----------

#This is the original dataset loaded from raw file. It may contain data from many event types and not all of them are compatible together
# Option 1: direct access to ADL
df1 = spark.read.csv("adl://adlstore01.azuredatalakestore.net/demo_data/201501-hubway-tripdata.csv", header=True)
df2 = spark.read.csv("adl://adlstore01.azuredatalakestore.net/demo_data/201504-hubway-tripdata.csv", header=True)
df = df1.union(df2)
df.write.mode("overwrite").save("adl://adlstore01.azuredatalakestore.net/demo_data/20150401-hubway-tripdata.csv",format="csv")
df.select("tripduration", "starttime", "stoptime").write.mode("overwrite").saveAsTable("20150401tripdata")

# Option 2: through mount point
# df = spark.read.format("com.databricks.spark.avro").load("dbfs:/mnt/avro/avro_test/2")
# df = df.na.fill('NA')

# COMMAND ----------

df3 = spark.sql("SELECT * FROM csv.`adl://adlstore01.azuredatalakestore.net/demo_data/201504-hubway-tripdata.csv` where _c9>41")
df3.show()

# COMMAND ----------

df.createOrReplaceTempView( "weather" )
sqlDF = spark.sql("SELECT REPORTTPYE, avg( HOURLYDRYBULBTEMPF ) FROM weather GROUP BY REPORTTPYE having REPORTTPYE <>'SOD'")
display(sqlDF)

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