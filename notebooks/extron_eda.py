# Databricks notebook source
import sklearn
print('The scikit-learn version is {}.'.format(sklearn.__version__))


# COMMAND ----------

invoice_df = spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("/mnt/extron/rawdata/Invoice.csv")
invoice_df.registerTempTable("Invoice_Header")
product_df =  spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("/mnt/extron/rawdata/Products.csv")
product_df.registerTempTable("Products")
Invoice_Line_Item_df =  spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("/mnt/extron/rawdata/InvoiceLineItem.csv")
Invoice_Line_Item_df.registerTempTable("Invoice_Line_Item")
Invoice_Line_Item_Component_df =  spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("/mnt/extron/rawdata/InvoiceLineItemComponent.csv")
Invoice_Line_Item_Component_df.registerTempTable("Invoice_Line_Item_Component")
RO_Detail_Repair_df =  spark.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").load("/mnt/extron/rawdata/RODetailAllRepair.csv")
RO_Detail_Repair_df.registerTempTable("RO_Detail_Repair")


# COMMAND ----------

# MAGIC %sql select * from products where skPartNumberId = 53931	

# COMMAND ----------

# MAGIC %sql select * from Invoice_Header where InvoiceId ='378562'

# COMMAND ----------

# MAGIC %sql select * from Invoice_Line_Item where InvoiceRecordId= '378562*329601*1'

# COMMAND ----------

# MAGIC %sql select * from Invoice_Line_Item_Component where InvoiceLineitemRecordId = '328925*287143*3*001'

# COMMAND ----------

# MAGIC %sql select * from RO_Detail_Repair, Invoice_Header where RO_Detail_Repair.INV_NO = Invoice_Header.InvoiceId and COUNTRY ='102'

# COMMAND ----------

# MAGIC %sql select * from Invoice_Header

# COMMAND ----------

# MAGIC %sql select SYMPTOM_CODE, CAUSE, SOLUTION, count(*) from   RO_Detail_Repair group by SYMPTOM_CODE, CAUSE, SOLUTION ORDER BY count(*) Desc

# COMMAND ----------

# MAGIC %sql select * from Invoice_Line_Item_Component,Invoice_Line_Item where Invoice_Line_Item.InvoiceLineitemRecordId=Invoice_Line_Item_Component.InvoiceLineitemRecordId

# COMMAND ----------

# MAGIC %sql select COUNTRY,WHS, cast(FAIL_QTY as int) as FAIL_QTY, to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) as Date_Receipt, RCVD_DT, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/365) as age from RO_Detail_Repair where REPLACE_PN='119167'  and WHS='003'  order by to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) 

# COMMAND ----------

# MAGIC %sql select count(*), COUNTRY, REPLACE_PN, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/365) as age from RO_Detail_Repair group by age, COUNTRY, REPLACE_PN having REPLACE_PN='119167'  order by age 

# COMMAND ----------

# MAGIC %sql select count(*), COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/365) as age from RO_Detail_Repair group by age, COUNTRY, PART having PART='66047'  order by age 

# COMMAND ----------

# MAGIC 
# MAGIC %sql select count(*) as count, COUNTRY, PART from RO_Detail_Repair group by  COUNTRY, PART  order by count desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP ))) as diff from RO_Detail_Repair

# COMMAND ----------

# MAGIC %sql select avg(count) from (select count(*) as count, REPLACE_PN from RO_Detail_Repair group by REPLACE_PN)

# COMMAND ----------

# MAGIC %sql select  REPLACE_PN, count from (select count(*) as count, REPLACE_PN from RO_Detail_Repair group by REPLACE_PN) order by count desc limit 30

# COMMAND ----------

# MAGIC %sql select count(*) from RO_Detail_Repair

# COMMAND ----------

# MAGIC %sql select  PART, count from (select count(*) as count, PART from RO_Detail_Repair group by PART) order by count desc limit 20

# COMMAND ----------

ro_df = spark.sql("select COUNTRY, PART, to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )) as ORIG_SHIPDT , to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) as RCVD_DT, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as age, CUST_ID, cast(ORDER_QTY as int) as ORDER_QTY, cast(FAIL_QTY as int) as FAIL_QTY, OPER, WHS from RO_Detail_Repair where  REPLACE_PN='116821' order by CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') as TIMESTAMP)")
ro_df.cache()

# COMMAND ----------

ro_df_119167 = spark.sql("select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as age, CUST_ID, cast(ORDER_QTY as int) as ORDER_QTY, cast(FAIL_QTY as int) as FAIL_QTY, OPER, WHS from RO_Detail_Repair where  REPLACE_PN='119167' order by CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') as TIMESTAMP)")
ro_df_119167.cache()
ro_df_pd_119167 = ro_df_119167.toPandas()


# COMMAND ----------

import pandas as pd

new_rows = []
for index, row in ro_df_pd_119167.iterrows():
  new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'], 'age': row['age'],'FAIL_QTY': 1})
  for age in range(int(row['age'])):
    new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'],'age': row['age'],'FAIL_QTY': 0})
new_ro_df_pd_119167 = pd.DataFrame(new_rows)
new_ro_df_pd_119167_encoded= pd.get_dummies(new_ro_df_pd_119167)

# COMMAND ----------

#This is the model for part 107944

ro_df_107944 = spark.sql("select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as age, CUST_ID, cast(ORDER_QTY as int) as ORDER_QTY, cast(FAIL_QTY as int) as FAIL_QTY, OPER, WHS from RO_Detail_Repair where  REPLACE_PN='107944' order by CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') as TIMESTAMP)")
ro_df_107944.cache()
ro_df_pd_107944 = ro_df_107944.toPandas()

import pandas as pd

new_rows = []
for index, row in ro_df_pd_107944.iterrows():
  new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'], 'age': row['age'],'FAIL_QTY': 1})
  for age in range(int(row['age'])):
    new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'],'age': row['age'],'FAIL_QTY': 0})
new_ro_df_pd_107944 = pd.DataFrame(new_rows)
new_ro_df_pd_107944_encoded= pd.get_dummies(new_ro_df_pd_107944)


from sklearn.ensemble import RandomForestRegressor

from sklearn.cross_validation import train_test_split




labels = new_ro_df_pd_107944['FAIL_QTY'].values
features = new_ro_df_pd_107944_encoded.values


trainingLabels, testLabels, trainingFeatures, testFeatures = train_test_split(labels, features, test_size=0.3)

fail_qty_model = RandomForestRegressor(max_depth=13, random_state=0,n_estimators=30)
fail_qty_model.fit(trainingFeatures,trainingLabels)
train_pred = fail_qty_model.predict(trainingFeatures)
test_pred = fail_qty_model.predict(testFeatures)

from sklearn.metrics import recall_score, accuracy_score

print('recall train: %.5f, test: %.5f' % (recall_score(trainingLabels, train_pred), recall_score(testLabels, test_pred)))
print('accuracy train: %.5f, test: %.5f' % (accuracy_score(trainingLabels, train_pred), accuracy_score(testLabels, test_pred)))

# COMMAND ----------

sum(test_pred)

# COMMAND ----------

def demand_forecast(part_no):
  #This is the model for part 107944
  pred_qty=[]
  actual_qty=[]
  months_duration = int(spark.sql("select round(datediff(max,min)/12) as months_duration from (select max(RCVD_DT) as max, min(RCVD_DT) as min from (select to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))").collect()[0][0])
  for month in range(12, 13):
    train_sql ="select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))"
    ro_df_train = spark.sql(train_sql)

# This is the training set, it will be increased as the process progress through time
    
#     ro_df.cache()
    ro_df_pd_train = ro_df_train.toPandas()
    ro_df_pred = spark.sql("select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month+1)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))")
#     ro_df.cache()
    ro_df_pd_pred = ro_df_pred.toPandas()

    import pandas as pd

    new_rows = []
    for index, row in ro_df_pd_train.iterrows():
      new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'], 'AGE': row['AGE'],'FAIL_QTY': 1})
      for age in range(int(row['AGE'])):
        new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'],'AGE': row['AGE'],'FAIL_QTY': 0})
    new_ro_df_pd_train = pd.DataFrame(new_rows)
    new_ro_df_pd_train_encoded= pd.get_dummies(new_ro_df_pd_train)
    
    ro_df_pd_pred_encoded =pd.get_dummies(ro_df_pd_pred)

    from sklearn.ensemble import RandomForestRegressor


    labels = new_ro_df_pd_train['FAIL_QTY'].values
    features = new_ro_df_pd_train_encoded.values
    
    actual_total_qty = ro_df_pd_pred['FAIL_QTY'].sum()
    actual_qty.appnd(actual_total_qty)
    pred_features = ro_df_pd_pred_encoded.values
    testLabels = ro_df_pd_pred['FAIL_QTY'].values

    fail_qty_model = RandomForestRegressor(max_depth=13, random_state=0,n_estimators=30)
    fail_qty_model.fit(features,labels)
    train_labels =fail_qty_model.predict(features)
    pred_labels =fail_qty_model.predict(pred_features)

    pred_total_qty = sum(pred_labels)
    
    from sklearn.metrics import recall_score, accuracy_score
    print("Result for month: ", month)
    print('recall train: %.5f, test: %.5f' % (recall_score(labels, train_labels), recall_score(testLabels, pred_labels)))
    print('accuracy train: %.5f, test: %.5f' % (accuracy_score(labels, train_labels), accuracy_score(testLabels, pred_labels)))
    
    pred_qty.append(pred_total_qty)
    
  return pred_qty, actual_qty



# COMMAND ----------

pred_qty, actual_qty = demand_forecast('107944')

# COMMAND ----------



# COMMAND ----------

from sklearn.preprocessing import LabelEncoder

part_no = '107944'
pred_qty=[]
actual_qty=[]
ohe_df = spark.sql("select COUNTRY, PART from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'")
ohe_df_pd =ohe_df.toPandas()
enc_country = LabelEncoder()
enc_part = LabelEncoder()
enc_part.fit(ohe_df_pd.values[:,1])
enc_country.fit(ohe_df_pd.values[:,0])

months_duration,starting_month = spark.sql("select round(datediff(max,min)/12) as months_duration, add_months(min,12) as starting_month from (select max(RCVD_DT) as max, min(RCVD_DT) as min from (select to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))").collect()[0]
print("month_duration is: ", months_duration)
print("starting_month is: ", starting_month)

for month in range(12, int(32)-12):

  train_sql ="select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))"
  ro_df_train = spark.sql(train_sql)

  # This is the training set, it will be increased as the process progress through time

  #     ro_df.cache()
  ro_df_pd_train = ro_df_train.toPandas()
  pred_sql ="SELECT * FROM (select A.COUNTRY, A.PART, A.AGE, 0 as FAIL_QTY from (select COUNTRY, PART, round(datediff(add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),1),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE  from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))) A left outer join (select COUNTRY, PART from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month+1)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))) B ON (A.COUNTRY = B.COUNTRY) AND (A.PART = B.PART) WHERE B.COUNTRY IS NULL) union all (select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month+1)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')))"
#   print(train_sql)
#   print(pred_sql)
  ro_df_pred = spark.sql(pred_sql)
  #     ro_df.cache()

  ro_df_pd_pred = ro_df_pred.toPandas()
  import pandas as pd

  new_rows = []
  for index, row in ro_df_pd_train.iterrows():
    new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'], 'AGE': row['AGE'],'FAIL_QTY': row['FAIL_QTY']})
    for age in range(int(row['AGE'])):
      new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'],'AGE': row['AGE'],'FAIL_QTY': 0})
  new_ro_df_pd_train = pd.DataFrame(new_rows, columns=['COUNTRY', 'PART', 'AGE', 'FAIL_QTY'] )
  new_ro_df_pd_train_encoded =new_ro_df_pd_train.values
  enc_country.transform(new_ro_df_pd_train_encoded[:,0])

  enc_part.transform(new_ro_df_pd_train_encoded[:,1])

  ro_df_pd_pred_encoded =ro_df_pd_pred.values
  enc_part.transform(ro_df_pd_pred_encoded[:,1])
  enc_country.transform(ro_df_pd_pred_encoded[:,0])



  from sklearn.ensemble import RandomForestRegressor


  labels = new_ro_df_pd_train['FAIL_QTY'].values
  features = new_ro_df_pd_train_encoded[:, 0:3]

  actual_total_qty = ro_df_pd_pred['FAIL_QTY'].sum()
  actual_qty.append(actual_total_qty)
  pred_features = ro_df_pd_pred_encoded[:,0:3]
  testLabels = ro_df_pd_pred['FAIL_QTY'].values

  fail_qty_model = RandomForestRegressor(max_depth=13, random_state=0,n_estimators=30)
  fail_qty_model.fit(features,labels)
  train_labels =fail_qty_model.predict(features)
  pred_labels =fail_qty_model.predict(pred_features)

  pred_total_qty = sum(pred_labels)

#   from sklearn.metrics import recall_score, accuracy_score
  print("Result for month: ", month)
  print("pred_total_qty: ", pred_total_qty, " actual_total_qty: ",actual_total_qty)
#   print('recall train: %.5f, test: %.5f' % (recall_score(labels, train_labels), recall_score(testLabels, pred_labels)))
#   print('accuracy train: %.5f, test: %.5f' % (accuracy_score(labels, train_labels), accuracy_score(testLabels, pred_labels)))

  pred_qty.append(pred_total_qty)

# COMMAND ----------

new_ro_df_pd_train_encoded[:,0:3]

# COMMAND ----------



# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
import numpy as np
part_no = '116821'
pred_qty=[]
actual_qty=[]
ohe_df = spark.sql("select COUNTRY, PART, WHS, 0 as AGE from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'")
ohe_df_pd =ohe_df.toPandas()
enc_country = LabelEncoder()
enc_part = LabelEncoder()
enc_whs = LabelEncoder()
ohe = OneHotEncoder(categorical_features=[0,1,2])

ohe.fit(ohe_df_pd.values)
enc_part.fit(ohe_df_pd.values[:,1])
enc_country.fit(ohe_df_pd.values[:,0])
enc_whs.fit(ohe_df_pd.values[:,2])


months_duration,starting_month = spark.sql("select round(datediff(max,min)/12) as months_duration, add_months(min,12) as starting_month from (select max(RCVD_DT) as max, min(RCVD_DT) as min from (select to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))").collect()[0]
print("month_duration is: ", months_duration)
print("starting_month is: ", starting_month)

for month in range(12, int(months_duration)-12):

  train_sql ="select COUNTRY, PART, WHS, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))"
  ro_df_train = spark.sql(train_sql)

  # This is the training set, it will be increased as the process progress through time

  #     ro_df.cache()
  ro_df_pd_train = ro_df_train.toPandas()
  pred_sql ="select COUNTRY, PART, WHS, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month+1)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))"

#   pred_sql ="SELECT * FROM (select A.COUNTRY, A.PART,A.WHS,  A.AGE, 0 as FAIL_QTY from (select COUNTRY, PART,WHS,  round(datediff(add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),1),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE  from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))) A left outer join (select COUNTRY, PART,WHS from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month+1)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"'))) B ON (A.COUNTRY = B.COUNTRY) AND (A.PART = B.PART) AND (A.WHS =B.WHS ) WHERE B.COUNTRY IS NULL AND B.PART IS NULL AND B.WHS IS NULL) union all (select COUNTRY, PART, WHS, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY from RO_Detail_Repair where REPLACE_PN ='"+part_no+"' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), "+str(month+1)+") as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='"+part_no+"')))"
#   print(pred_sql)
  ro_df_pred = spark.sql(pred_sql)
  #     ro_df.cache()

  ro_df_pd_pred = ro_df_pred.toPandas()
  import pandas as pd

#   new_rows = []
#   for index, row in ro_df_pd_train.iterrows():
#     new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'],'WHS':row['WHS'], 'AGE': row['AGE'],'FAIL_QTY': row['FAIL_QTY']})
#     for age in range(int(row['AGE'])):
#       new_rows.append({'COUNTRY':row['COUNTRY'], 'PART':row['PART'],'WHS':row['WHS'],'AGE': row['AGE'],'FAIL_QTY': 0})
#   new_ro_df_pd_train = pd.DataFrame(new_rows, columns=['COUNTRY', 'PART','WHS', 'AGE', 'FAIL_QTY'] )
  train_features =ro_df_pd_train[['COUNTRY', 'PART','WHS', 'AGE']].values

  train_features[:,0]=enc_country.transform(train_features[:,0])
  
  train_features[:,1]=enc_part.transform(train_features[:,1])
  train_features[:,2] =enc_whs.transform(train_features[:,2])
  train_ohe_features=ohe.transform(train_features)
  features =train_ohe_features
  
  pred_features =ro_df_pd_pred[['COUNTRY', 'PART','WHS', 'AGE']].values

  pred_features[:,0]=enc_country.transform(pred_features[:,0])
  
  pred_features[:,1]=enc_part.transform(pred_features[:,1])
  pred_features[:,2] =enc_whs.transform(pred_features[:,2])
  pred_ohe_features=ohe.transform(pred_features)
  pred_features = pred_ohe_features


  from sklearn.ensemble import RandomForestRegressor


  labels = ro_df_pd_train['FAIL_QTY'].values


  actual_total_qty = ro_df_pd_pred['FAIL_QTY'].sum()
  actual_qty.append(actual_total_qty)
  
  testLabels = ro_df_pd_pred['FAIL_QTY'].values

  fail_qty_model = RandomForestRegressor(max_depth=9, random_state=0,n_estimators=30)
  fail_qty_model.fit(features,labels)
  train_labels =fail_qty_model.predict(features)
  pred_labels =fail_qty_model.predict(pred_features)

  pred_total_qty = sum(pred_labels)

#   from sklearn.metrics import recall_score, accuracy_score
  print("Result for month: ", month)
  print("pred_total_qty: ", pred_total_qty, " actual_total_qty: ",actual_total_qty)
#   print('recall train: %.5f, test: %.5f' % (recall_score(labels, train_labels), recall_score(testLabels, pred_labels)))
#   print('accuracy train: %.5f, test: %.5f' % (accuracy_score(labels, train_labels), accuracy_score(testLabels, pred_labels)))

  pred_qty.append(pred_total_qty)

# COMMAND ----------

train_ohe_features

# COMMAND ----------

# MAGIC %sql select count(*) from RO_Detail_Repair where REPLACE_PN ='107944'

# COMMAND ----------



import matplotlib.pyplot as plt
plt.clf()
plt.plot(pred_qty,  '.')
plt.plot(actual_qty,  'o')

plt.xlabel('time line')
plt.ylabel('quantity')
display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY, WHS from RO_Detail_Repair where to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) < (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), 12) as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='107944'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select COUNTRY, PART, round(datediff(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )),to_date(CAST(UNIX_TIMESTAMP(ORIG_SHIPDT, 'MM/dd/yy') AS TIMESTAMP )))/12) as AGE,  cast(FAIL_QTY as int) as FAIL_QTY, WHS from RO_Detail_Repair where to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) <= (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), 13) as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='107944')) and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) > (select min(RCVD_DT) from( select add_months(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )), 12) as RCVD_DT from RO_Detail_Repair where REPLACE_PN ='107944'))