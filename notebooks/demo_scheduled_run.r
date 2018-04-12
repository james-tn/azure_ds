# Databricks notebook source
library(sparklyr)

require(RODBC)

# establish a connection using the DSN you created earlier
conn <- odbcConnect("databricks")

# run a SQL query using the connection you created
res <- sqlQuery(conn, "SELECT * FROM fm_12_tbl")

# print out the column names in the query output
names(res) 

# print out the number of rows in the query output
nrow (res)

res <- sqlQuery(conn, "CREATE table new_tbl as (SELECT * FROM fm_12_tbl)")




# COMMAND ----------

library(SparkR)
df <- read.df("/mnt/demo/csv/201501-hubway-tripdata.csv",
                    source = "csv", header="true", inferSchema = "true")
df$duration_hours <- df$tripduration / 60

df <- select(df, "duration_hours","starttime")
df <- filter(df, df$duration_hours >2)
write.df(df, path="dbfs:/mnt/demo/csv/duration.parquet", source="parquet", mode="overwrite")

#update from R
#Another update
#yes, this is ok from databricks

# COMMAND ----------

df <- filter(df, df$duration_hours >3)
display(df)