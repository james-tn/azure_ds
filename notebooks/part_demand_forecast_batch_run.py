# Databricks notebook source
# This part is to connect to SQL server database and query data
# This part is to connect to SQL server database and query data
jdbcUsername='extron'
jdbcPassword='machinelearning@2018'
jdbcHostname = "extronforecast.database.windows.net"
#The new DB is restored in this DB. Forecast output is stored in forecast DB
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

# product_query = "(select * from dimProducts) product_alias"
# Product_df = spark.read.jdbc(url=jdbcUrl, table=product_query, properties=connectionProperties)
# Product_df.write.mode("overwrite").saveAsTable("Products")
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
#Set the end date to limit the forecast horizon This can be parameterized later
end_date ='09/01/2018'
country_list = spark.sql("select country from (select  country, count(*) from RO_Detail_repair where country<> '' group by country order by count(*) desc limit 60)").collect()
country_list = [i[0] for i in country_list]

select_invoice = "select substring(Accountid,0,2) operation,  to_date(CAST(UNIX_TIMESTAMP(concat(month(to_date(CAST(UNIX_TIMESTAMP(InvoiceDate, 'MM/dd/yy') AS TIMESTAMP ))),'"+'-'+"', year(to_date(CAST(UNIX_TIMESTAMP(InvoiceDate, 'MM/dd/yy') AS TIMESTAMP )))), 'MM-yyyy') AS TIMESTAMP ))  as month, PartNumber, sum(ShipQuantity) ShipQuantity from (select InvoiceRecordId,  PartNumber, sum(ShipQuantity) ShipQuantity from Invoice_Line_Item where  ShipQuantity >0  group by InvoiceRecordId,  PartNumber) A, Invoice_Header B where B.InvoiceRecordId = A.InvoiceRecordId group by operation, month, PartNumber"
invoice_df = spark.sql(select_invoice)
end_date =datetime.strptime(end_date,'%m/%d/%Y')

all_time_df = invoice_df.rdd.flatMap(lambda row: [(row[0],row[1], x, row[2],row[3]) for x in [dt.date() for dt in rrule(MONTHLY, dtstart=row[1], until=end_date)]]).toDF(["operation", "Month", "Date", "PartNumber", "ShipQuantity"])


all_time_df.registerTempTable("all_time_products")
total_dataset_df = spark.sql("select A.*, (datediff(Month, Date)/365) as Age, B.REPLACE_PN, B.FAIL_QTY from all_time_products A left outer join (select replace_pn, fail_qty,RCVD_DT,  substring(Cust_id,0,2) operation, part, to_date(CAST(UNIX_TIMESTAMP(concat(month(to_date(CAST(UNIX_TIMESTAMP(Sales, 'MM/dd/yy') AS TIMESTAMP ))),'"+'-'+"', year(to_date(CAST(UNIX_TIMESTAMP(Sales, 'MM/dd/yy') AS TIMESTAMP )))), 'MM-yyyy') AS TIMESTAMP ))  as sales_month from RO_Detail_Repair where replace_pn <> '' and bill_code ='3') B on A.PartNumber = B.Part and A.operation = B.operation and A.month = b.sales_month and month(A.Date) = month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))) and year(A.Date) = year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )))")
total_dataset_df.write.mode("overwrite").saveAsTable("total_dataset")

# COMMAND ----------

#This is to generate data for the time series model (ts). This is not used for regression model
beg_month = '01-2001'
end_date ='09/01/2018'

from datetime import datetime
import time
from dateutil.rrule import rrule, MONTHLY
from pyspark.sql import Row
import pyspark.sql.functions as F
#Get the list of invoices and start_dates
import pandas as pd
from pyspark.sql.functions import when


forecastData = spark.sql("select substring(Cust_id,0,2) operation, replace_pn,to_date(CAST(UNIX_TIMESTAMP(concat(month(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP ))),'"+'-'+"', year(to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )))), 'MM-yyyy') AS TIMESTAMP ))  as month, sum(fail_qty) fail_qty  from RO_Detail_Repair where  to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) >= to_date(CAST(UNIX_TIMESTAMP('"+beg_month+"', 'MM-yyyy') AS TIMESTAMP))  group by month, replace_pn, operation").dropna()

added_data = forecastData.filter(forecastData.month =='2018-06-01')
end_date =datetime.strptime(end_date,'%m/%d/%Y')
added_data = added_data.rdd.flatMap(lambda row: [(row[0],row[1],x, 0) for x in [dt.date() for dt in rrule(MONTHLY, dtstart=row[2], until=end_date)]]).toDF(["operation", "replace_pn", "month", "fail_qty"])
forecastData = forecastData.unionAll(added_data)
forecastData = forecastData.groupby("operation","replace_pn", "month").agg({"fail_qty":"sum"}).withColumnRenamed("sum(Fail_Qty)", "Fail_Qty")
forecastData.cache()



# COMMAND ----------

#This is time series model based on RNN architecture. It's purely based on temporal data of the variable to forecast (univariate)
def forecast_ts(v,test_start_dt):
  import pandas as pd
  from sklearn.preprocessing import MinMaxScaler
  from dateutil.relativedelta import relativedelta
  import datetime as dt

  from keras.models import Model, Sequential
  from keras.layers import GRU, Dense
  from keras.callbacks import EarlyStopping
  
  def create_evaluation_df(predictions, test_inputs, H, scaler):
    eval_df = pd.DataFrame(predictions, columns=['t+'+str(t) for t in range(1, H+1)])
    eval_df['timestamp'] = test_inputs.dataframe.index
    eval_df = pd.melt(eval_df, id_vars='timestamp', value_name='prediction', var_name='h')
    eval_df['actual'] = np.transpose(test_inputs['target']).ravel()
    eval_df[['prediction', 'actual']] = scaler.inverse_transform(eval_df[['prediction', 'actual']])
    return eval_df
  import numpy as np
  from collections import UserDict
  import pandas as pd
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

          for name, structure in self.tensor_structure.items():
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
  
  
  
  operation = v[0][0]
  part = v[0][1]
  

  labels = ['date', 'fail_qty']
  forecastData = pd.DataFrame.from_records(list(v[1]), columns=labels).set_index('date')
  forecastData.index = pd.to_datetime(forecastData.index)
  forecastData =forecastData.sort_index()
  HORIZON = 3
  result_df = pd.DataFrame() #creates a new dataframe that's empty to return rolling forecast result
  real_start_dt = dt.datetime.strptime(test_start_dt, '%Y-%m-%d')
  train = forecastData.copy()[forecastData.index < real_start_dt][['fail_qty']]
  T = min(12, train.shape[0]-2)
  LATENT_DIM = 5
  BATCH_SIZE = 32
  EPOCHS = 100
  if T <=0: 
    print("T is small ") 
    return [(0, 0,0,0,operation, part)]



#   if train.shape[0]<T+2:
#     #no result
#     return [(0, 0,0,0,operation, part)]



  y_scaler = MinMaxScaler()
  y_scaler.fit(train[['fail_qty']])
  X_scaler = MinMaxScaler()
  train[['fail_qty']] = X_scaler.fit_transform(train[['fail_qty']])
  tensor_structure = {'X':(range(-T+1, 1), ['fail_qty'])}
  train_inputs = TimeSeriesTensor(dataset=train,
                              target='fail_qty',
                              H=HORIZON,
                              tensor_structure=tensor_structure,

                              drop_incomplete=True)






  model = Sequential()
  model.add(GRU(LATENT_DIM, input_shape=(T, 1)))
  model.add(Dense(HORIZON))

  model.compile(optimizer='RMSprop', loss='mse')
  model.summary()

#   earlystop = EarlyStopping(monitor='val_loss', min_delta=0, patience=120)
  history = model.fit(train_inputs['X'],
                      train_inputs['target'],
                      batch_size=BATCH_SIZE,
                                                epochs=EPOCHS,

#                     validation_data=(valid_inputs['X'], valid_inputs['target']),
#                       callbacks=[earlystop],
                      verbose=0)


  look_back_dt = real_start_dt - relativedelta(months=T)
  test = forecastData.copy()[(forecastData.index >=look_back_dt)& (forecastData.index <= real_start_dt+relativedelta(months=2))][['fail_qty']]
  if test.shape[0]>T+2:
    test[['fail_qty']] = X_scaler.transform(test[['fail_qty']])
    test_inputs = TimeSeriesTensor(test, 'fail_qty', HORIZON, tensor_structure)

    predictions = model.predict(test_inputs['X'])

    result_df = create_evaluation_df(predictions, test_inputs, HORIZON, y_scaler)



    #   plot_df = eval_df.resample('M', on = 'timestamp').sum().reset_index()
    #   print("forecast for ",real_start_dt)
    #   print(plot_df)
  #   if real_start_dt >=dt.datetime.strptime(end_dt, '%Y-%m-%d')-relativedelta(months=2): break
  else:
      return [(0, 0,0,0,operation, part)]
  
  result_df['timestamp']=result_df.apply(lambda x: x['timestamp'] + pd.DateOffset(months = 3) if x.h=='t+3' else (x['timestamp'] + pd.DateOffset(months = 2) if x.h=='t+2' else x['timestamp'] + pd.DateOffset(months = 1)), axis=1)

  result_df.timestamp = (result_df.timestamp).astype(str)
  result_df['operation']=operation
  result_df['part'] = part

 
  

  return result_df.values.tolist()

test_start_dt = '2018-03-01'

  
forecastData.rdd.map(lambda row: ((row[0],row[1]),(row[2], row[3]))).groupByKey().flatMap(lambda x: forecast_ts(x, test_start_dt)).filter(lambda row: row[0]!=0).toDF(["timestamp", "h", "prediction", "actual", "operation", "part"]).registerTempTable('result_ts')

sql= "select 'ts' as type, operation, part as replace_PN,  CASE WHEN h = 't+1' THEN  1 ELSE CASE WHEN h = 't+2' THEN 2 ELSE 3 END END as Forecast_Month, to_date(timestamp) as date, prediction as forecast, actual  FROM result_ts" 
spark.sql(sql).write.jdbc(url=jdbcUrl, table="forecast_result", mode="append", properties=connectionProperties)



# COMMAND ----------

# This is the 2nd model based on random foreacst regression to estimate the demand based on products quantity sold and their history of repair demand. The model foreacst for t+1, t+2, t+3
def forecast_rg(v,test_start_dt):
  import pandas as pd
  from sklearn.preprocessing import MinMaxScaler
  from datetime import datetime
  import numpy as np
  from sklearn.ensemble import RandomForestRegressor
  from dateutil.relativedelta import relativedelta



  
  
  format_str = '%Y-%m-%d' # The format for start_month


  operation = v[0][0]
  part = v[0][1]
  t1,t2,t3=0,0,0
  t1_actual, t2_actual, t3_actual =0,0,0



  labels = ["date", "part", "age", "ship_qty", "fail_qty" ]
  forecastData = pd.DataFrame.from_records(list(v[1]), columns=labels).set_index('date')
  forecastData.index = pd.to_datetime(forecastData.index)
  forecastData =forecastData.sort_index()
  nrows = forecastData.shape[0]
  #assuming n-3 will be the number of training data

  start_month = datetime.strptime(test_start_dt, format_str)
#   end_limit = start_month+relativedelta(months=-14)
  t1_month = start_month+relativedelta(months=1)
  t2_month = start_month+relativedelta(months=2)
  t3_month = start_month+relativedelta(months=3)



  if nrows>3:


    forecastData = pd.get_dummies(forecastData)
    trainData = forecastData[(forecastData.index<=start_month)]
    if trainData.shape[0]==0:
        return [(operation, part,1,t1_month.strftime('%Y-%m-%d'), t1,t1_actual)]


    x_train = trainData.drop(["fail_qty"], axis=1)
    y_train = trainData["fail_qty"]





        
    t1_forecastData= forecastData[forecastData.index==t1_month].drop(["fail_qty"], axis=1)
    t2_forecastData= forecastData[forecastData.index==t2_month].drop(["fail_qty"], axis=1)

    t3_forecastData= forecastData[forecastData.index==t3_month].drop(["fail_qty"], axis=1)
    





    regr = RandomForestRegressor(max_depth=18, random_state=0)
    model =regr.fit(x_train, y_train)
    if t1_forecastData.shape[0]>0:
      t1=np.sum(model.predict(t1_forecastData)).item()
      t1_actual= np.sum(forecastData[forecastData.index==t1_month]["fail_qty"]).item()



    if t2_forecastData.shape[0]>0:
      t2= np.sum(model.predict(t2_forecastData)).item()
      t2_actual= np.sum(forecastData[forecastData.index==t2_month]["fail_qty"]).item()


    if t3_forecastData.shape[0]>0:
      t3= np.sum(model.predict(t3_forecastData)).item()
      t3_actual= np.sum(forecastData[forecastData.index==t3_month]["fail_qty"]).item()


  return [(operation, part,1,t1_month.strftime('%Y-%m-%d'), t1,t1_actual),(operation, part, 2,t2_month.strftime('%Y-%m-%d'), t2,t2_actual), (operation, part,3, t3_month.strftime('%Y-%m-%d'), t3,t3_actual)] 

  


# COMMAND ----------

#run training and forecast for regression model with beg_month means limit about the length of history to look at for training. Test_start_date is the start month (t) to produce the forecast
beg_month='01-2000'
test_start_dt = '2018-04-01'

from pyspark.sql.functions import when

 # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

# this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be #increased by one month in the next loop and so on till there's no actual data in the start month.
 

rg_dataset = spark.sql("select operation,replace_pn_t, date, PartNumber, bround(Age,1) as Age,sum(ShipQuantity) ShipQuantity, sum(cast(fail_qty as int)) as fail_qty    from (select total_dataset.operation, total_dataset.month, total_dataset.date, total_dataset.partnumber, total_dataset.shipquantity, total_dataset.age, total_dataset.replace_pn, case when total_dataset.replace_pn = a.replace_pn then total_dataset.fail_qty else 0 end as fail_qty,a.replace_pn as replace_pn_t from total_dataset,  (select distinct part, replace_pn from ro_detail_repair where bill_code ='3' and replace_pn <> '' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) >= to_date(CAST(UNIX_TIMESTAMP('"+beg_month+"', 'MM-yyyy') AS TIMESTAMP ) ))  a where total_dataset.partnumber = a.part and total_dataset.date >= to_date(CAST(UNIX_TIMESTAMP('"+beg_month+"', 'MM-yyyy') AS TIMESTAMP )) ) where date > add_months(to_date(CAST(UNIX_TIMESTAMP('"+test_start_dt+"', 'yyyy-MM-dd') AS TIMESTAMP )), -100)  group by operation,replace_pn_t, date, PartNumber, Age")
rg_dataset.rdd.map(lambda row: ((row[0],row[1]),(row[2], row[3],row[4],row[5],row[6]))).groupByKey().flatMap(lambda x: forecast_rg(x, test_start_dt)).filter(lambda row: row[4]!=0).toDF(["operation", "part", "h", "timestamp", "prediction", "actual"]).registerTempTable('result_rg')

sql= "select 'rg' as type, operation, part as replace_PN,  h as Forecast_Month, to_date(timestamp) as date, prediction as forecast, actual  FROM result_rg" 
spark.sql(sql).write.jdbc(url=jdbcUrl, table="forecast_result", mode="append", properties=connectionProperties)

# COMMAND ----------

#just a different start month
beg_month='01-2000'
test_start_dt = '2018-05-01'

from pyspark.sql.functions import when

 # this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be increased by one month in the next loop and so on till there's no actual data in the start month.

# this is the start month to perform rolling forecast in which we use data from before start month as the training data then forecast data in the start month. Then start month will be #increased by one month in the next loop and so on till there's no actual data in the start month.
 

rg_dataset = spark.sql("select operation,replace_pn_t, date, PartNumber, bround(Age,1) as Age,sum(ShipQuantity) ShipQuantity, sum(cast(fail_qty as int)) as fail_qty    from (select total_dataset.operation, total_dataset.month, total_dataset.date, total_dataset.partnumber, total_dataset.shipquantity, total_dataset.age, total_dataset.replace_pn, case when total_dataset.replace_pn = a.replace_pn then total_dataset.fail_qty else 0 end as fail_qty,a.replace_pn as replace_pn_t from total_dataset,  (select distinct part, replace_pn from ro_detail_repair where bill_code ='3' and replace_pn <> '' and to_date(CAST(UNIX_TIMESTAMP(RCVD_DT, 'MM/dd/yy') AS TIMESTAMP )) >= to_date(CAST(UNIX_TIMESTAMP('"+beg_month+"', 'MM-yyyy') AS TIMESTAMP ) ))  a where total_dataset.partnumber = a.part and total_dataset.date >= to_date(CAST(UNIX_TIMESTAMP('"+beg_month+"', 'MM-yyyy') AS TIMESTAMP )) ) where date > add_months(to_date(CAST(UNIX_TIMESTAMP('"+test_start_dt+"', 'yyyy-MM-dd') AS TIMESTAMP )), -100)  group by operation,replace_pn_t, date, PartNumber, Age")
rg_dataset.rdd.map(lambda row: ((row[0],row[1]),(row[2], row[3],row[4],row[5],row[6]))).groupByKey().flatMap(lambda x: forecast_rg(x, test_start_dt)).filter(lambda row: row[4]!=0).toDF(["operation", "part", "h", "timestamp", "prediction", "actual"]).registerTempTable('result_rg')

sql= "select 'rg' as type, operation, part as replace_PN,  h as Forecast_Month, to_date(timestamp) as date, prediction as forecast, actual  FROM result_rg" 
spark.sql(sql).write.jdbc(url=jdbcUrl, table="forecast_result", mode="append", properties=connectionProperties)

# COMMAND ----------

# MAGIC %sql refresh table invoice_line_item

# COMMAND ----------

