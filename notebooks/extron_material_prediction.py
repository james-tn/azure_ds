# Databricks notebook source
# This part is to connect to SQL server database and query data
jdbcUsername='your username'
jdbcPassword='your jdbc pass'
jdbcHostname = "msdemosql001.database.windows.net"
jdbcDatabase = "MachineLearningMS"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
print(jdbcUrl)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

#Store SQL data tables into Databricks tables so that we do not depend on SQL server later on. This can be run again should there be a need to refresh data from source
invoice_line_query = "(select * from InvoiceLineItem) invoiceLine_alias"
Invoice_Line_Item_df = spark.read.jdbc(url=jdbcUrl, table=invoice_line_query, properties=connectionProperties)
Invoice_Line_Item_df.write.mode("overwrite").saveAsTable("Invoice_Line_Item")

invoice_query = "(select * from Invoice) invoice_alias"
Invoice_df = spark.read.jdbc(url=jdbcUrl, table=invoice_query, properties=connectionProperties)
Invoice_df.write.mode("overwrite").saveAsTable("Invoice_Header")

RO_query = "(select * from RODetail) RO_alias"
RO_Detail_Repair_df = spark.read.jdbc(url=jdbcUrl, table=RO_query, properties=connectionProperties)
RO_Detail_Repair_df.write.mode("overwrite").saveAsTable("RO_Detail_Repair")

product_query = "(select * from dimProducts) product_alias"
Product_df = spark.read.jdbc(url=jdbcUrl, table=product_query, properties=connectionProperties)
Product_df.write.mode("overwrite").saveAsTable("Products")
#BOM data is not copied due to data format problem and it is not needed for now
# bom_query = "(select * from BOMs) bom_alias"
# BOM_df = spark.read.jdbc(url=jdbcUrl, table=bom_query, properties=connectionProperties)
# BOM_df.write.mode("overwrite").saveAsTable("BOM")

inventory_query = "(select * from Inventory) inventory_alias"
Inventory_df = spark.read.jdbc(url=jdbcUrl, table=inventory_query, properties=connectionProperties)
Inventory_df.write.mode("overwrite").saveAsTable("Inventory")

# COMMAND ----------

#   This procedure is to generate exteneded examples of both positive examples (instances of repair service from RO detail) as well as negative examples which are instances of 
# products in the months where Extron did not receive request for repair. We use month as the unit of time to aggregate data. The basis is to join RODetail data and invoice data then generate additional data for months where there were no repair. The data is for all producs ever sold in the invoice data. Output data is saved in a table for subsequent processes. Just need to run this cell once for all parts
from datetime import datetime
import time
from dateutil.rrule import rrule, MONTHLY
from pyspark.sql import Row
import pyspark.sql.functions as F
#Get the list of invoices and start_dates
end_date ='06/01/2018'

select_invoice = "select ShipToCountry country,  to_date(CAST(UNIX_TIMESTAMP(concat(month(to_date(CAST(UNIX_TIMESTAMP(InvoiceDate, 'MM/dd/yy') AS TIMESTAMP ))),'"+'-'+"', year(to_date(CAST(UNIX_TIMESTAMP(InvoiceDate, 'MM/dd/yy') AS TIMESTAMP )))), 'MM-yyyy') AS TIMESTAMP ))  as month, PartNumber, sum(ShipQuantity) ShipQuantity from (select InvoiceRecordId,  PartNumber, sum(ShipQuantity) ShipQuantity from Invoice_Line_Item where  ShipQuantity >0  group by InvoiceRecordId,  PartNumber) A, Invoice_Header B where B.InvoiceRecordId = A.InvoiceRecordId group by ShipToCountry, month, PartNumber"
invoice_df = spark.sql(select_invoice)
end_date =datetime.strptime(end_date,'%m/%d/%Y')

all_time_df = invoice_df.rdd.flatMap(lambda row: [(row[0],row[1], x, row[2],row[3]) for x in [dt.date() for dt in rrule(MONTHLY, dtstart=row[1], until=end_date)]]).toDF(["COUNTRY", "Month", "Date", "PartNumber", "ShipQuantity"])


all_time_df.registerTempTable("all_time_products")
total_dataset_df = spark.sql("select A.*, (datediff(Month, Date)/365) as Age, B.REPLACE_PN, B.FAIL_QTY from all_time_products A left outer join (select replace_pn, fail_qty,RCVD_DT, country, part, to_date(CAST(UNIX_TIMESTAMP(concat(month(to_date(CAST(UNIX_TIMESTAMP(Sales, 'MM/dd/yy') AS TIMESTAMP ))),'"+'-'+"', year(to_date(CAST(UNIX_TIMESTAMP(Sales, 'MM/dd/yy') AS TIMESTAMP )))), 'MM-yyyy') AS TIMESTAMP ))  as sales_month from RO_Detail_Repair where replace_pn <> '' and bill_code ='3') B on A.PartNumber = B.Part and A.COUNTRY = B.COUNTRY and A.month = b.sales_month and month(A.Date) = month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) and year(A.Date) = year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )))")
total_dataset_df.write.mode("overwrite").saveAsTable("total_dataset")

# COMMAND ----------

total_dataset_df = spark.sql("select A.*, (datediff(Month, Date)/365) as Age, B.REPLACE_PN, B.FAIL_QTY from all_time_products A left outer join (select replace_pn, fail_qty,RCVD_DT, country, part, to_date(CAST(UNIX_TIMESTAMP(concat(month(to_date(CAST(UNIX_TIMESTAMP(Sales, 'MM/dd/yy') AS TIMESTAMP ))),'"+'-'+"', year(to_date(CAST(UNIX_TIMESTAMP(Sales, 'MM/dd/yy') AS TIMESTAMP )))), 'MM-yyyy') AS TIMESTAMP ))  as sales_month from RO_Detail_Repair where replace_pn <> '' and bill_code ='3') B on A.PartNumber = B.Part and A.COUNTRY = B.COUNTRY and A.month = b.sales_month and month(A.Date) = month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) and year(A.Date) = year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )))")
total_dataset_df.write.mode("overwrite").saveAsTable("total_dataset")

# COMMAND ----------

# MAGIC %sql select * from total_dataset where replace_pn is not null

# COMMAND ----------

#   2nd version, not based on invoice id as link between sales data and RO data.

#This procedure is to generate exteneded examples of both positive examples (instances of repair service from RO detail) as well as negative examples which are instances of 
# products in the months where Extron did not receive request for repair. We use month as the unit of time to aggregate data. The basis is to join RODetail data and invoice data then generate additional data for months where there were no repair. The data is for all producs ever sold in the invoice data. Output data is saved in a table for subsequent processes. Just need to run this cell once for all parts
from datetime import datetime
import time
from dateutil.rrule import rrule, MONTHLY
from pyspark.sql import Row
import pyspark.sql.functions as F
#Get the list of invoices and start_dates
end_date ='06/01/2018'

select_invoice = "select  ShipToCountry, to_date(CAST(UNIX_TIMESTAMP(InvoiceDate, 'MM/dd/yy') AS TIMESTAMP )) as InvoiceDate, PartNumber, ShipQuantity from (select InvoiceRecordId,  PartNumber, sum(ShipQuantity) ShipQuantity from Invoice_Line_Item where  ShipQuantity >0  group by InvoiceRecordId,  PartNumber) A, Invoice_Header B where B.InvoiceRecordId = A.InvoiceRecordId  and InvoiceId is not Null and to_date(CAST(UNIX_TIMESTAMP(InvoiceDate, 'MM/dd/yy') AS TIMESTAMP )) <to_date(CAST(UNIX_TIMESTAMP('"+end_date+"', 'MM/dd/yy') AS TIMESTAMP ))"
print(select_invoice)
invoice_df = spark.sql(select_invoice)
end_date =datetime.strptime(end_date,'%m/%d/%Y')

all_time_df = invoice_df.rdd.flatMap(lambda row: [(row[0],row[1],row[2], x, row[3],row[4]) for x in [dt.date() for dt in rrule(MONTHLY, dtstart=row[2], until=end_date)]]).toDF(["InvoiceId","COUNTRY", "InvoiceDate", "Date", "PartNumber", "ShipQuantity"])


all_time_df.registerTempTable("all_time_products")
total_dataset_df = spark.sql("select A.*, (datediff(Date, InvoiceDate)/365) as Age, B.ORDER_QTY as REPAIR_QTY, B.REPLACE_PN, B.FAIL_QTY, B.REASON,B.CAUSE from all_time_products A left outer join RO_Detail_Repair B on A.PartNumber = B.Part and A.COUNTRY = B.COUNTRY and month(A.Date) = month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) and year(A.Date) = year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) and A.InvoiceId = B.INV_NO")
total_dataset_df.write.mode("overwrite").saveAsTable("total_dataset")

# COMMAND ----------

from pyspark.sql.functions import when

Replace_PN ='15-173-85LF'
# start_month = '01-2015' # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

part_list = [i[0] for i in spark.sql("select distinct PART from RO_Detail_Repair where REPLACE_PN = '"+Replace_PN+"'").collect()]
total_dataset_df = spark.sql("select * from total_dataset")
one_part_dataset = total_dataset_df.filter(total_dataset_df["PartNumber"].isin(part_list))
one_part_dataset = one_part_dataset.withColumn("fail_qty", when(one_part_dataset.REPLACE_PN ==Replace_PN,cast(one_part_dataset.FAIL_QTY as float)).otherwise(0))
one_part_dataset.registerTempTable("one_part_dataset_1")


# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor

from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator

from pyspark.sql.functions import when
mldataset = spark.sql("select COUNTRY, PartNumber, bround(Age) as Age_round, sum(ShipQuantity) as Ship_Qty, sum(cast(fail_qty as float)) Fail_Qty from one_part_dataset_1 where COUNTRY <>'' and PartNumber <>'' group by COUNTRY, PartNumber, Age_round ")

trainingData, testData = mldataset.randomSplit([0.8,0.2])

# non_na_dataset = trainingData.filter(trainingData.fail_qty !=0)
# for i in range(1,2):
#   non_na_dataset_samples = non_na_dataset.sample(True, 0.9)
#   trainingData = trainingData.union(non_na_dataset_samples)

  # labelIndexer = StringIndexer(inputCol="REPLACE_PN", outputCol="part").fit(dataset) 
countryIndexer = StringIndexer(inputCol="COUNTRY", outputCol="cn").fit(mldataset)
productIndexer = StringIndexer(inputCol="PartNumber", outputCol="product").fit(mldataset)

encoder = OneHotEncoderEstimator(inputCols=["cn", "product","Age_round"],
                                 outputCols=["countryVec", "productVec","ageVec"])

assembler = VectorAssembler(
  inputCols=["ageVec","Ship_Qty","countryVec","productVec"],
  outputCol="features")






  # featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(dataset4)
  # Split the data into training and test sets (30% held out for testing)
  # Train a RandomForest model.
rf =RandomForestRegressor(labelCol="Fail_Qty", featuresCol="features")

# Convert indexed labels back to original labels.
# labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
#                                labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
rf_pipeline = Pipeline(stages=[countryIndexer,productIndexer,encoder, assembler, rf])

# Train model.  This also runs the indexers.
rf_model = rf_pipeline.fit(trainingData)

# Make predictions.
rf_predictions = rf_model.transform(testData)

rf_predictions.select("prediction", "Fail_Qty", "features").show(50)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="Fail_Qty", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(rf_predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

rfModel = rf_model.stages[1]
print(rfModel)  # summary only



# # Select (prediction, true label) and compute test error
# evaluator = MulticlassClassificationEvaluator(
#     labelCol="fail_qty", predictionCol="prediction", metricName="weightedRecall")
# rf_accuracy = evaluator.evaluate(rf_predictions)
# print("Test Error = %g" % (1.0 - rf_accuracy))

# rfModel = rf_model.stages[2]
# print(rfModel)  # summary only

# COMMAND ----------

rf_predictions = rf_model.transform(testData)

rf_predictions.filter(rf_predictions.Fail_Qty>0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #This SQL is to test for a given replace part if there's sufficient data matching between invoice data and repair data.

# COMMAND ----------

# MAGIC %sql select A.match_count, B.ro_count repair_order_count,  A.Country, A.REPLACE_PN from  (select count(*) match_count, REPLACE_PN, COUNTRY from total_dataset  where REPLACE_PN is not null  group by COUNTRY, REPLACE_PN order by count(*) desc) A, (select count(*) ro_count, REPLACE_PN, COUNTRY from RO_Detail_Repair where REPLACE_PN is not null and REPLACE_PN <> '' and bill_code ='3' group by COUNTRY, REPLACE_PN order by count(*) desc) B where A.REPLACE_PN = B.REPLACE_PN and A.COUNTRY = B.COUNTRY  order by match_count desc limit  60

# COMMAND ----------

# MAGIC %sql select count(INV_NO) from RO_Detail_Repair where INV_NO not in (select InvoiceId from Invoice_Header)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### This is to show data for a particular part which is '28-210-01LF' that will be used to train an example forecast model

# COMMAND ----------

# MAGIC 
# MAGIC %sql select A.match_count, B.ro_count,  A.Country, A.REPLACE_PN from  (select count(*) match_count, REPLACE_PN, COUNTRY from total_dataset  where REPLACE_PN is not null  group by COUNTRY, REPLACE_PN order by count(*) desc) A, (select count(*) ro_count, REPLACE_PN, COUNTRY from RO_Detail_Repair where REPLACE_PN is not null and bill_code ='3'   group by COUNTRY, REPLACE_PN order by count(*) desc) B where A.REPLACE_PN = B.REPLACE_PN and A.COUNTRY = B.COUNTRY and A.REPLACE_PN = '28-210-01LF' order by match_count desc 

# COMMAND ----------

# MAGIC 
# MAGIC %sql select A.match_count, B.ro_count,  A.Country, A.REPLACE_PN from  (select count(*) match_count, REPLACE_PN, COUNTRY from total_dataset  where REPLACE_PN is not null  group by COUNTRY, REPLACE_PN order by count(*) desc) A, (select count(*) ro_count, REPLACE_PN, COUNTRY from RO_Detail_Repair where REPLACE_PN is not null  group by COUNTRY, REPLACE_PN order by count(*) desc) B where A.REPLACE_PN = B.REPLACE_PN and A.COUNTRY = B.COUNTRY and A.REPLACE_PN = '11-000-82LF' order by match_count desc 

# COMMAND ----------

# MAGIC %sql drop table result_month

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor

from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator

from pyspark.sql.functions import when

Replace_PN ='28-210-01LF'
start_month = '01-2015' # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

# this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be #increased by one month in the next loop and so on till there's no actual data in the start month.

part_list = [i[0] for i in spark.sql("select distinct PART from RO_Detail_Repair where REPLACE_PN = '"+Replace_PN+"' and bill_code ='3' and COuntry ='UNITED STATES' ").collect()]
total_dataset_df = spark.sql("select * from total_dataset where COuntry ='UNITED STATES'")
one_part_dataset = total_dataset_df.filter(total_dataset_df["PartNumber"].isin(part_list))
one_part_dataset = one_part_dataset.withColumn("fail_qty", when(one_part_dataset.REPLACE_PN ==Replace_PN,one_part_dataset.FAIL_QTY).otherwise(0))
one_part_dataset.registerTempTable("one_part_dataset")

data_avail = True
while (True):
  forecastData = spark.sql("select  date, PartNumber, bround(Age,1) as Age,ShipQuantity, cast(fail_qty as int) from one_part_dataset A where  date  <add_months(to_date(CAST(UNIX_TIMESTAMP('"+start_month+"', 'MM-yyyy') AS TIMESTAMP )),1) and date >= to_date(CAST(UNIX_TIMESTAMP('"+start_month+"', 'MM-yyyy') AS TIMESTAMP )) and COUNTRY  <>'' and PartNumber <> '' ").dropna()
  if(forecastData.filter(forecastData.ShipQuantity>0).count()==0): break

#   mldataset = spark.sql("select  date, PartNumber, bround(Age,1) as Age,ShipQuantity, cast(fail_qty as int) from one_part_dataset A where  date  <add_months(to_date(CAST(UNIX_TIMESTAMP('"+start_month+"', 'MM-yyyy') AS TIMESTAMP )),1) and year(date) >2008").dropna()
  trainingData = spark.sql("select date, PartNumber, bround(Age,1) as Age,ShipQuantity, cast(fail_qty as int) from one_part_dataset A where  date  <to_date(CAST(UNIX_TIMESTAMP('"+start_month+"', 'MM-yyyy') AS TIMESTAMP )) and year(date) >2008 and COUNTRY  <>'' and PartNumber <> '' ").dropna()
  mldataset = trainingData.union(forecastData)
  month, year = start_month.split("-")
  if (int(month)==12):
    year = int(year)+1
    month=1
  else:month = int(month)+1
  start_month =str(month)+ "-"+str(year)




#   non_na_dataset = trainingData.filter(trainingData.fail_qty !=0)
#   for i in range(1,70):
#     non_na_dataset_samples = non_na_dataset.sample(True, 0.9)
#     trainingData = trainingData.union(non_na_dataset_samples)

  # labelIndexer = StringIndexer(inputCol="REPLACE_PN", outputCol="part").fit(dataset) 
# //   countryIndexer = StringIndexer(inputCol="COUNTRY", outputCol="cn").fit(mldataset)
  productIndexer = StringIndexer(inputCol="PartNumber", outputCol="product").fit(mldataset)

  encoder = OneHotEncoderEstimator(inputCols=["product"],
                                   outputCols=[ "productVec"])

  assembler = VectorAssembler(
      inputCols=["Age","ShipQuantity","productVec"],
      outputCol="features")



  # featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(dataset4)
  # Split the data into training and test sets (30% held out for testing)
  # Train a RandomForest model.
  rf = RandomForestRegressor(labelCol="fail_qty", featuresCol="features", numTrees=10,maxBins =170)

  # Convert indexed labels back to original labels.
  # labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
  #                                labels=labelIndexer.labels)

  # Chain indexers and forest in a Pipeline
  rf_pipeline = Pipeline(stages=[productIndexer,encoder, assembler, rf])

  # Train model.  This also runs the indexers.
  rf_model = rf_pipeline.fit(trainingData)

  # Make predictions.
  rf_predictions = rf_model.transform(forecastData)


# # Select (prediction, true label) and compute test error
# evaluator = MulticlassClassificationEvaluator(
#     labelCol="fail_qty", predictionCol="prediction", metricName="weightedRecall")
# rf_accuracy = evaluator.evaluate(rf_predictions)
# print("Test Error = %g" % (1.0 - rf_accuracy))

# rfModel = rf_model.stages[2]
# print(rfModel)  # summary only

  from datetime import datetime
  import time
  from dateutil.rrule import rrule, MONTHLY
  from pyspark.sql import Row
  import pyspark.sql.functions as F
  #Get the list of invoices and start_dates



  result_df = rf_predictions.rdd.map(lambda row: (Replace_PN, row[0],row[1],row[2],row[3],row[4], row[8])).toDF(["REPLACE_PN",  "Date", "PartNumber", "Age","ShipQuantity", "Fail_Qty", "Prediction"])

  from pyspark.sql.functions import concat, col, lit, month, year, bround

  result_df_month =result_df.orderBy("Date").groupBy("REPLACE_PN","date").agg({"Fail_Qty":"sum","Prediction":"sum", "ShipQuantity":"sum"}).withColumnRenamed("sum(Fail_Qty)", "Fail_Qty").withColumnRenamed("sum(Prediction)", "prediction").withColumnRenamed("sum(ShipQuantity)", "Total_ShipQuantity")
  result_df_month.write.mode("append").saveAsTable("result_month7")

# COMMAND ----------

# MAGIC %sql drop table result_month7

# COMMAND ----------

#   import datetime
from datetime import datetime
import time
from dateutil.rrule import rrule, MONTHLY
from pyspark.sql import Row
import pyspark.sql.functions as F
#Get the list of invoices and start_dates



result_df = rf_predictions.rdd.map(lambda row: (Replace_PN, row[0],row[1],row[2],row[3],row[4],row[5], row[11])).toDF(["REPLACE_PN", "Country", "Date", "PartNumber", "Age","ShipQuantity", "Fail_Qty", "Prediction"])

from pyspark.sql.functions import concat, col, lit, month, year, bround

result_df_month =result_df.orderBy("Country", "Date").groupBy("REPLACE_PN", "Country","date").agg({"Fail_Qty":"sum","Prediction":"sum", "ShipQuantity":"sum"}).withColumnRenamed("sum(Fail_Qty)", "Fail_Qty").withColumnRenamed("sum(Prediction)", "prediction").withColumnRenamed("sum(ShipQuantity)", "Total_ShipQuantity")
result_df_month.write.mode("append").saveAsTable("result_month7")


# COMMAND ----------



# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator

from pyspark.sql.functions import when

Replace_PN ='28-210-01LF'
start_month = '01-2015' # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

part_list = [i[0] for i in spark.sql("select distinct PART from RO_Detail_Repair where REPLACE_PN = '"+Replace_PN+"'").collect()]
total_dataset_df = spark.sql("select * from total_dataset")
one_part_dataset = total_dataset_df.filter(total_dataset_df["PartNumber"].isin(part_list))
one_part_dataset = one_part_dataset.withColumn("fail_qty", when(one_part_dataset.REPLACE_PN ==Replace_PN,one_part_dataset.FAIL_QTY).otherwise(0))
one_part_dataset.registerTempTable("one_part_dataset")


# COMMAND ----------

# MAGIC %sql select sum(fail_qty) from one_part_dataset 

# COMMAND ----------

len(part_list)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='28-210-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='28-210-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month4 A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='28-210-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='28-210-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month5 A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='28-210-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month6 A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='27-555-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction,Org_Prediction, month from result_month A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='15-173-85LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month3 A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

Replace_PN ='15-173-85LF'
start_month = '01-2015' # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

part_list = [i[0] for i in spark.sql("select distinct PART from RO_Detail_Repair where REPLACE_PN = '"+Replace_PN+"'").collect()]
total_dataset_df = spark.sql("select * from total_dataset")
one_part_dataset = total_dataset_df.filter(total_dataset_df["PartNumber"].isin(part_list))
one_part_dataset = one_part_dataset.withColumn("fail_qty", when(one_part_dataset.REPLACE_PN ==Replace_PN,one_part_dataset.FAIL_QTY).otherwise(0))
one_part_dataset.registerTempTable("one_part_dataset")


# COMMAND ----------

# MAGIC %sql select * from RO_Detail_Repair where INV_NO='1894109' and REPLACE_PN ='11-000-82LF'

# COMMAND ----------

# MAGIC %sql select * from Invoice_Header where InvoiceId='1894109'

# COMMAND ----------

# MAGIC %sql select * from Invoice_Line_Item where InvoiceRecordId ='1894109*266673*0'

# COMMAND ----------

# MAGIC %sql select sum(FAIL_QTY),month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) month,year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) year from RO_Detail_Repair where REPLACE_PN ='11-000-82LF' and Country = 'UNITED STATES' group by month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))),year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )))  order by year, month

# COMMAND ----------

#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='11-000-82LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as int) actual_qty, cast(New_Prediction as int) prediction, month from result_month A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part)
display(part_pred_result)

# COMMAND ----------

# select_sql="select sum(FAIL_QTY) as qty, REPLACE_PN, COUNTRY, WHS, to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) as date from RO_Detail_Repair group by  COUNTRY, WHS,REPLACE_PN, date having REPLACE_PN='38-188-01LF' and COUNTRY ='UNITED STATES' order by date"

# pd = spark.sql(select_sql).toPandas()
# pd['date'] = pd['date'].astype('datetime64[ns]')
# df = pd.set_index('date')
# ts = df['qty']
# tsm = ts.dropna().resample('2W').sum().dropna()



#After running model training and forecast generation above for parts, run this cell to compare result of forecast vs. actual using rolling forecast method.Run this per country
Replace_PN ='28-210-01LF' 
Country = 'UNITED STATES'
sql_part = "select cast(Fail_Qty as float) actual_qty, cast(New_Prediction as int) prediction, to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP )) as month from result_month6 A where  Country = '"+Country+"' and REPLACE_PN ='"+Replace_PN+"' order by to_date(CAST(UNIX_TIMESTAMP(Month, 'MM-yyyy') AS TIMESTAMP ))"
part_pred_result = spark.sql(sql_part).toPandas()

part_pred_result['month'] = part_pred_result['month'].astype('datetime64[ns]')
part_df = part_pred_result.set_index('month')
tsm = part_df[['actual_qty', ]


# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.sql.functions import concat, col, lit, month, year, bround

from pyspark.sql.functions import when

Replace_PN ='28-210-01LF'
Country = 'UNITED STATES'

start_month = '01-2011' # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

part_list = [i[0] for i in spark.sql("select distinct PART from RO_Detail_Repair where REPLACE_PN = '"+Replace_PN+"'").collect()]
total_dataset_df = spark.sql("select COUNTRY, PartNumber,Age, cast(FAIL_QTY as float), ShipQuantity, REPLACE_PN, Date from total_dataset where  Country = '"+Country+"'")
one_part_dataset = total_dataset_df.filter(total_dataset_df["PartNumber"].isin(part_list))
one_part_dataset = one_part_dataset.withColumn("fail_qty", when(one_part_dataset.REPLACE_PN ==Replace_PN,one_part_dataset.FAIL_QTY).otherwise(0))
# one_part_dataset =one_part_dataset.withColumn("month", concat( weekofyear(col("date")), lit("-"), year(col("date"))))

one_part_dataset.registerTempTable("one_part_dataset")

forecastData = spark.sql("select date, sum(fail_qty) fail_qty  from one_part_dataset A where  date >to_date(CAST(UNIX_TIMESTAMP('"+start_month+"', 'MM-yyyy') AS TIMESTAMP )) and COUNTRY  <>'' and PartNumber <> '' group by  date ").dropna()

# COMMAND ----------

display(one_part_dataset)

# COMMAND ----------

import pandas as pd
forecastData =forecastData.toPandas().set_index('date')
forecastData.index = pd.to_datetime(forecastData.index)


# COMMAND ----------

forecastData

# COMMAND ----------

valid_start_dt = '2017-01-01 00:00:00'
test_start_dt = '2017-07-01 00:00:00'
T = 6
HORIZON = 1
train = forecastData.copy()[forecastData.index < valid_start_dt][['fail_qty']]


# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler

y_scaler = MinMaxScaler()
y_scaler.fit(train[['fail_qty']])
X_scaler = MinMaxScaler()
train[['fail_qty']] = X_scaler.fit_transform(train[['fail_qty']])


# COMMAND ----------


tensor_structure = {'X':(range(-T+1, 1), ['fail_qty'])}
train_inputs = TimeSeriesTensor(dataset=train,
                            target='fail_qty',
                            H=HORIZON,
                            tensor_structure=tensor_structure,
                                                        freq='D',

                            drop_incomplete=True)

# COMMAND ----------

train_inputs.dataframe.head(100)


# COMMAND ----------

import datetime as dt

look_back_dt = dt.datetime.strptime(valid_start_dt, '%Y-%m-%d %H:%M:%S') - dt.timedelta(hours=T-1)
valid = forecastData.copy()[(forecastData.index >=look_back_dt) & (forecastData.index < test_start_dt)][['fail_qty']]
valid[['fail_qty']] = X_scaler.transform(valid[['fail_qty']])
valid_inputs = TimeSeriesTensor(valid, 'fail_qty', HORIZON, tensor_structure)


# COMMAND ----------



# COMMAND ----------

from pandas import datetime
import matplotlib.pyplot as plt
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error

size = int(len(X) * 0.7)
train, test = X[0:size], X[size:len(X)]
history = [x for x in train]
predictions = list()
for t in range(len(test)):
	model = ARIMA(history, order=(8, 1, 1))
	model_fit = model.fit(disp=0)
	output = model_fit.forecast()
	yhat = output[0]
	predictions.append(yhat)
	obs = test[t]
	history.append(obs)
	print('predicted=%f, expected=%f' % (yhat, obs))
error = mean_squared_error(test, predictions)
print('Test MSE: %.3f' % error)
# plot
plt.clf()

pyplot.plot(test)
pyplot.plot(predictions, color='red')
pyplot.show()
display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## This is the implementation using Recurrent Neural Net

# COMMAND ----------

import numpy as np
from collections import UserDict

class TimeSeriesTensor(UserDict):
    
    # A dictionary of tensors for input into the RNN model
    
    # Use this class to:
    #   1. Shift the values of the time series to create a Pandas dataframe containing all the data
    #      for a single training example
    #   2. Discard any samples with missing values
    #   3. Transform this Pandas dataframe into a numpy array of shape 
    #      (samples, time steps, features) for input into Keras

    # The class takes the following parameters:
    #    - **dataset**: original time series
    #    - **H**: the forecast horizon
    #    - **tensor_structures**: a dictionary discribing the tensor structure of the form
    #          { 'tensor_name' : (range(max_backward_shift, max_forward_shift), [feature, feature, ...] ) }
    #          if features are non-sequential and should not be shifted, use the form
    #          { 'tensor_name' : (None, [feature, feature, ...])}
    #    - **freq**: time series frequency
    #    - **drop_incomplete**: (Boolean) whether to drop incomplete samples
    
    def __init__(self, dataset, target, H, tensor_structure, freq='H', drop_incomplete=True):
        self.dataset = dataset
        self.target = target
        self.tensor_structure = tensor_structure
        self.tensor_names = list(tensor_structure.keys())
        
        self.dataframe = self._shift_data(H, freq, drop_incomplete)
        self.data = self._df2tensors(self.dataframe)
    
    
    def _shift_data(self, H, freq, drop_incomplete):
        print("Hello shift")
        
        # Use the tensor_structures definitions to shift the features in the original dataset.
        # The result is a Pandas dataframe with multi-index columns in the hierarchy
        #     tensor - the name of the input tensor
        #     feature - the input feature to be shifted
        #     time step - the time step for the RNN in which the data is input. These labels
        #         are centred on time t. the forecast creation time
        df = self.dataset.copy()
        
        idx_tuples = []
        for t in range(1, H+1):
            df['t+'+str(t)] = df[self.target].shift(t*-1)
            idx_tuples.append(('target', 'y', 't+'+str(t)))
            print("First shift", t)

        for name, structure in self.tensor_structure.items():
            print("Name is", name)
            rng = structure[0]
            dataset_cols = structure[1]
            
            for col in dataset_cols:
            
            # do not shift non-sequential 'static' features
                if rng is None:
                    df['context_'+col] = df[col]
                    idx_tuples.append((name, col, 'static'))

                else:
                    for t in rng:
                        sign = '+' if t > 0 else ''
                        shift = str(t) if t != 0 else ''
                        period = 't'+sign+shift
                        shifted_col = name+'_'+col+'_'+period
#                         print("second shift",shifted_col)
                        df[shifted_col] = df[col].shift(t*-1)
#                         print(df[shifted_col])
#                         print("then shift,",t*-1, freq)
#                         print( df[col].shift(t*-1, freq='M'))
                        idx_tuples.append((name, col, period))
                
        df = df.drop(self.dataset.columns, axis=1)
        idx = pd.MultiIndex.from_tuples(idx_tuples, names=['tensor', 'feature', 'time step'])
        df.columns = idx

        if drop_incomplete:
            df = df.dropna(how='any')

        return df
    
    
    def _df2tensors(self, dataframe):
        
        # Transform the shifted Pandas dataframe into the multidimensional numpy arrays. These
        # arrays can be used to input into the keras model and can be accessed by tensor name.
        # For example, for a TimeSeriesTensor object named "model_inputs" and a tensor named
        # "target", the input tensor can be acccessed with model_inputs['target']
    
        inputs = {}
        y = dataframe['target']
        y = y.as_matrix()
        inputs['target'] = y

        for name, structure in self.tensor_structure.items():
            rng = structure[0]
            cols = structure[1]
            tensor = dataframe[name][cols].as_matrix()
            if rng is None:
                tensor = tensor.reshape(tensor.shape[0], len(cols))
            else:
                tensor = tensor.reshape(tensor.shape[0], len(cols), len(rng))
                tensor = np.transpose(tensor, axes=[0, 2, 1])
            inputs[name] = tensor

        return inputs
    
    
    def subset_data(self, new_dataframe):
        
        # Use this function to recreate the input tensors if the shifted dataframe
        # has been filtered.
        
        self.dataframe = new_dataframe
        self.data = self._df2tensors(self.dataframe)

def create_evaluation_df(predictions, test_inputs, H, scaler):
    eval_df = pd.DataFrame(predictions, columns=['t+'+str(t) for t in range(1, H+1)])
    eval_df['timestamp'] = test_inputs.dataframe.index
    eval_df = pd.melt(eval_df, id_vars='timestamp', value_name='prediction', var_name='h')
    eval_df['actual'] = np.transpose(test_inputs['target']).ravel()
    eval_df[['prediction', 'actual']] = scaler.inverse_transform(eval_df[['prediction', 'actual']])
    return eval_df

def mape(predictions, actuals):
    return ((predictions - actuals).abs() / actuals).mean()

# COMMAND ----------



# COMMAND ----------

from collections import UserDict
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime as dt
from collections import UserDict

pd.options.display.float_format = '{:,.2f}'.format
np.set_printoptions(precision=2)


# COMMAND ----------

from keras.models import Model, Sequential
from keras.layers import GRU, Dense
from keras.callbacks import EarlyStopping


# COMMAND ----------

LATENT_DIM = 5
BATCH_SIZE = 32
EPOCHS = 2000


# COMMAND ----------

model = Sequential()
model.add(GRU(LATENT_DIM, input_shape=(T, 1)))
model.add(Dense(HORIZON))


# COMMAND ----------

model.compile(optimizer='RMSprop', loss='mse')
model.summary()


# COMMAND ----------

earlystop = EarlyStopping(monitor='val_loss', min_delta=0, patience=15)
history = model.fit(train_inputs['X'],
                    train_inputs['target'],
                    batch_size=BATCH_SIZE,
                    epochs=EPOCHS,
                    validation_data=(valid_inputs['X'], valid_inputs['target']),
                    verbose=1)


# COMMAND ----------

look_back_dt = dt.datetime.strptime(valid_start_dt, '%Y-%m-%d %H:%M:%S') - dt.timedelta(hours=T-1)
test = forecastData.copy()[valid_start_dt:][['fail_qty']]
test[['fail_qty']] = X_scaler.transform(test[['fail_qty']])
test_inputs = TimeSeriesTensor(test, 'fail_qty', HORIZON, tensor_structure)


# import datetime as dt

# look_back_dt = dt.datetime.strptime(valid_start_dt, '%Y-%m-%d %H:%M:%S') - dt.timedelta(hours=T-1)
# valid = forecastData.copy()[(forecastData.index >=look_back_dt) & (forecastData.index < test_start_dt)][['fail_qty','age','ShipQuantity']]
# valid[['age','ShipQuantity']] = X_scaler.transform(valid[['age','ShipQuantity']])
# valid_inputs = TimeSeriesTensor(valid, 'fail_qty', HORIZON, tensor_structure)
test_inputs.dataframe.head(100)


# COMMAND ----------

predictions = model.predict(test_inputs['X'])


# COMMAND ----------

eval_df = create_evaluation_df(predictions, test_inputs, HORIZON, y_scaler)


# COMMAND ----------


plot_df = eval_df.resample('M', on = 'timestamp').sum().reset_index()
plot_df

# COMMAND ----------

# MAGIC %sql select sum(fail_qty) , month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) as month, year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) as year from RO_Detail_Repair where REPLACE_PN = '19-2582-02LF' and COUNTRY = 'UNITED STATES' and Bill_code ='3' group by year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))), month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) order by year, month

# COMMAND ----------

# MAGIC %sql select sum(Fail_qty) from RO_Detail_Repair where REPLACE_PN = '19-2582-02LF' and COUNTRY = 'UNITED STATES'  and  Bill_code ='3' and INV_NO in (select invoiceId from Invoice_Header) and PART in (select PartNumber from Invoice_Line_Item, Invoice_Header,RO_Detail_Repair where Invoice_Line_Item.InvoiceRecordId = Invoice_Header.InvoiceRecordId and RO_Detail_Repair.INV_NO = Invoice_Header.Invoiceid and REPLACE_PN = '19-2582-02LF' and COUNTRY = 'UNITED STATES' a and Bill_code ='3') 

# COMMAND ----------

# MAGIC %sql select sum(Fail_qty) from RO_Detail_Repair, (select PartNumber, InvoiceId from Invoice_Header,Invoice_Line_Item where Invoice_Line_Item.InvoiceRecordId = Invoice_Header.InvoiceRecordId group by InvoiceId, Partnumber) B where REPLACE_PN = '28-210-01LF' and COUNTRY = 'UNITED STATES' and year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) =2017 and month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )))  = 10 and  Bill_code ='3' and   RO_Detail_Repair.INV_NO = B.Invoiceid and Bill_code ='3' and PART = B.PartNumber

# COMMAND ----------

# MAGIC %sql select sum(fail_qty) from one_part_dataset  where REPLACE_PN = '28-210-01LF' and COUNTRY = 'UNITED STATES' and year(to_date(CAST(UNIX_TIMESTAMP(Date, 'MM/dd/yy') AS TIMESTAMP ))) =2017 and month(to_date(CAST(UNIX_TIMESTAMP(Date, 'MM/dd/yy') AS TIMESTAMP )))  = 10

# COMMAND ----------

import matplotlib.pyplot as plt

plot_df.plot(x='timestamp', y=['prediction', 'actual'], style=['r', 'b'], figsize=(15, 8), fontsize=12)
plt.xlabel('timestamp', fontsize=12)
plt.ylabel('Demand', fontsize=12)
plt.show()
display()