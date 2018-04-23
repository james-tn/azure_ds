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
                add_headers(Authorization = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkZTaW11RnJGTm9DMHNKWEdtdjEzbk5aY2VEYyIsImtpZCI6IkZTaW11RnJGTm9DMHNKWEdtdjEzbk5aY2VEYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDcvIiwiaWF0IjoxNTI0NDU2Mzg1LCJuYmYiOjE1MjQ0NTYzODUsImV4cCI6MTUyNDQ2MDI4NSwiYWlvIjoiWTJkZ1lOaDdkYytMYWNtZDhwTkwrMDN1OWExV0FBQT0iLCJhcHBpZCI6ImFmODgzYWJmLTg5ZGQtNDg4OS1iZGIzLTFlZTg0ZjY4NDY1ZSIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0Ny8iLCJvaWQiOiI2MWZjMmZhNy01YzYxLTQxN2YtYTIwOS1mYTBkZmZjMGI5NDgiLCJzdWIiOiI2MWZjMmZhNy01YzYxLTQxN2YtYTIwOS1mYTBkZmZjMGI5NDgiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1dGkiOiI2U2lNcmt2RlVrQ01OaFduYjR4dUFBIiwidmVyIjoiMS4wIn0.lMfDNqycBmlkwOD9q5DKhLhmA2xpx7M4oTEBXGjYSCQJUa6kyusdkWT_vXAfp4c5nZ7OYal-nvxsEZXtSe0m05Dy1zy1SN6wI5nyKpZMFg7b6ytifob75LumALFaFmH-ubg0CuvoYJjvasPbnHWygD2ocloABCqnBwOosqdznFWZ6OMopCEBnANOLq89nBR2T0aut8D2a_k7PRy3scZsGsoapQDtmSsccE1iX_ABGs_wUf-18P75dxEJK8GN7RlPWICUDKaBfEkTl33LP8D0VcLDAPWGKK_XE49xnhoVFn6nMuh72ncjq4xV25DbDA2_YZXxEfAWC_PrpJK-kKKXPg"))
CSVData<-read.csv(text=(content(r, 'text')))

# writeBin(content(r), "C:/Users/azureuser/Downloads/iris.csv")
# irisDownloaded <- read.csv("C:/Users/azureuser/Downloads/iris.csv")
head(CSVData)
in_CSVData <- as.vector(CSVData, mode ='numeric')
r2 <- POST("https://adlstore01.azuredatalakestore.net/webhdfs/v1/demo_data/201501-hubway-tripdata_in.csv?op=OPEN&read=true",
                add_headers(Authorization = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkZTaW11RnJGTm9DMHNKWEdtdjEzbk5aY2VEYyIsImtpZCI6IkZTaW11RnJGTm9DMHNKWEdtdjEzbk5aY2VEYyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldC8iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDcvIiwiaWF0IjoxNTI0NDU2Mzg1LCJuYmYiOjE1MjQ0NTYzODUsImV4cCI6MTUyNDQ2MDI4NSwiYWlvIjoiWTJkZ1lOaDdkYytMYWNtZDhwTkwrMDN1OWExV0FBQT0iLCJhcHBpZCI6ImFmODgzYWJmLTg5ZGQtNDg4OS1iZGIzLTFlZTg0ZjY4NDY1ZSIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0Ny8iLCJvaWQiOiI2MWZjMmZhNy01YzYxLTQxN2YtYTIwOS1mYTBkZmZjMGI5NDgiLCJzdWIiOiI2MWZjMmZhNy01YzYxLTQxN2YtYTIwOS1mYTBkZmZjMGI5NDgiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1dGkiOiI2U2lNcmt2RlVrQ01OaFduYjR4dUFBIiwidmVyIjoiMS4wIn0.lMfDNqycBmlkwOD9q5DKhLhmA2xpx7M4oTEBXGjYSCQJUa6kyusdkWT_vXAfp4c5nZ7OYal-nvxsEZXtSe0m05Dy1zy1SN6wI5nyKpZMFg7b6ytifob75LumALFaFmH-ubg0CuvoYJjvasPbnHWygD2ocloABCqnBwOosqdznFWZ6OMopCEBnANOLq89nBR2T0aut8D2a_k7PRy3scZsGsoapQDtmSsccE1iX_ABGs_wUf-18P75dxEJK8GN7RlPWICUDKaBfEkTl33LP8D0VcLDAPWGKK_XE49xnhoVFn6nMuh72ncjq4xV25DbDA2_YZXxEfAWC_PrpJK-kKKXPg"), body = upload_file)


# COMMAND ----------

CSVData


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
