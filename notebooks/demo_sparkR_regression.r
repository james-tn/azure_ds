# Databricks notebook source
install.packages("httr", dependencies = TRUE)


# COMMAND ----------

tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
client_id = 'af883abf-89dd-4889-bdb3-1ee84f68465e'
client_secret = 'qId6BcZ6z03/Z5W9lSbuLMjPvfTF4yBpVAxrBoJHVBE='

# COMMAND ----------

library(httr)
library(jsonlite)
library(curl)
h <- new_handle()
handle_setform(h,
               "grant_type"="client_credentials",
               "resource"="https://management.core.windows.net/",
               "client_id"=client_id,
               "client_secret"=client_secret
)
req <- curl_fetch_memory("https://login.windows.net/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token", handle = h)
res <- fromJSON(rawToChar(req$content))
res$access_token


# COMMAND ----------

library(httr)
r <- httr::GET("https://adlstore01.azuredatalakestore.net/webhdfs/v1/demo_data/201501-hubway-tripdata.csv?op=OPEN&read=true",
                add_headers(Authorization = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkZTaW11RnJGTm9DMHNKWEdtdjEzbk5aY2VEYyIsImtpZCI6IkZTaW11RnJGTm9DMHNKWEdtdjEzbk5aY2VEYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDcvIiwiaWF0IjoxNTI0NDUxNDIzLCJuYmYiOjE1MjQ0NTE0MjMsImV4cCI6MTUyNDQ1NTMyMywiYWlvIjoiWTJkZ1lManY5MGU4N090M05zOEdsYWRSU2JzOEFBPT0iLCJhcHBpZCI6ImFmODgzYWJmLTg5ZGQtNDg4OS1iZGIzLTFlZTg0ZjY4NDY1ZSIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0Ny8iLCJvaWQiOiI2MWZjMmZhNy01YzYxLTQxN2YtYTIwOS1mYTBkZmZjMGI5NDgiLCJzdWIiOiI2MWZjMmZhNy01YzYxLTQxN2YtYTIwOS1mYTBkZmZjMGI5NDgiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1dGkiOiJnVl9WNzRJc2RFV1BnNXVqVVU1aUFBIiwidmVyIjoiMS4wIn0.UdXfMoEDIw6cl7St4GHmsisRtGEzYbQA_dlwBF6zHDZ9DluJCtbLGymr7culIIY-W5B3Ca_QEYN0G11Ek5-YHPdslvQvSJ2HPnWzYQDCk9XmeSL4sm15-4-VeeJxlgQ4oOdoH9Px1txbR3RXQ8XvcZ4OpvnTDgy3i5W1UtXllKd8C_Fs2TzS82ajJK8rB7ny7orXrt5T9mwMzMJeE0tN-yg8xbZ7bqS4LkvLpgbzRkEPD_9CgyrUodIG43-UtfjJMzCMBdLmmhbE9rVcVJnqe_HBM1OWWP20qOOP1dQKd4dPaNp81s_qoH1GktMvcUxiK0KmnZEM4C_nFjAw9PRPOQ"))
CSVData<-content(r)

# writeBin(content(r), "C:/Users/azureuser/Downloads/iris.csv")
# irisDownloaded <- read.csv("C:/Users/azureuser/Downloads/iris.csv")
head(CSVData)


# COMMAND ----------


library(SparkR)
df <- read.df("/mnt/demo/csv/201501-hubway-tripdata.csv",
                    source = "csv", header="true", inferSchema = "true")
df$duration_hours <- df$tripduration / 60

df <- select(df, "duration_hours","starttime")
df <- filter(df, df$duration_hours >2)
write.df(df, path="dbfs:/mnt/demo/csv/duration.parquet", source="parquet", mode="overwrite")


# COMMAND ----------

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

# Fit a linear model over the dataset.
model <- glm(tripduration ~ bikeid + gender, data = df, family = "gaussian")

# Model coefficients are returned in a similar format to R's native glm().
summary(model)

# COMMAND ----------

install.packages('deSolve')

install.packages('FME')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hubway_tripdata201501 

# COMMAND ----------

library(SparkR)
df <- sql("SELECT * FROM fm_12_tbl")
showDF(df)

# COMMAND ----------

# MAGIC %fs ls /mnt/demo/csv

# COMMAND ----------

library(sparklyr)
sc <- spark_connect(method = "databricks") 
