# Databricks notebook source
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

# COMMAND ----------

display(df)
