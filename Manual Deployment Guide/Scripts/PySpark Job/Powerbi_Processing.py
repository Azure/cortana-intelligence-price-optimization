
# coding: utf-8

# In[1]:

################################################## 0: import modules, create Spark Context and define functions
from pyspark import SparkConf
from pyspark import SparkContext 
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from datetime import datetime, timedelta
import itertools
import subprocess
sc = SparkContext()
sqlContext = HiveContext(sc)


# In[2]:

################################################## 1: define paths of input files and output files
## adl Name
adl_name="<Azuredatalakestore-Name>"
## adl link
adl_loc="adl://"+adl_name+".azuredatalakestore.net/"
## input paths
df_time_loc=adl_loc+"powerbi_data/time_data/"
df_model_performance_loc=adl_loc+"powerbi_data/model_performance_data/"
df_sales_loc=adl_loc+"aggregated_sales_data/"
opt_results_d_loc=adl_loc+"opt_results_data/"
## output paths
df_time_loc_text=adl_loc+"powerbi_data/csv_data/time_data.csv"
df_model_performance_loc_text=adl_loc+"powerbi_data/csv_data/model_performance_data.csv"
df_sales_loc_text=adl_loc+"powerbi_data/csv_data/aggregated_sales_data.csv"
df_opt_loc_text=adl_loc+"powerbi_data/csv_data/opt_data.csv"


# In[80]:

################################################## 2: read into data
df_time=sqlContext.read.parquet(df_time_loc)
df_model_performance=sqlContext.read.parquet(df_model_performance_loc)
df_sales=sqlContext.read.parquet(df_sales_loc)
df_opt=sqlContext.read.parquet(opt_results_d_loc)


# In[81]:

################################################## 3: deal with the df_time
## make the model training zero when model not trained
exec_date_list=df_time.select('exec_date').distinct().collect()
exec_date_list=[item.exec_date for item in exec_date_list]
step_list=df_time.select('step').distinct().collect()
step_list=[item.step for item in step_list]
df_time_all=sqlContext.createDataFrame(sc.parallelize([list(element) for element in itertools.product(exec_date_list,step_list)]),StructType([StructField('exec_date',StringType(),True),StructField('step',StringType(),True)]))
df_time=df_time.join(df_time_all,['step','exec_date'],'outer')
df_time=df_time.fillna({'time': 0})


# In[4]:

################################################## 4: write to output
dir_exists=subprocess.call(["hadoop", "fs", "-test", "-d", df_time_loc_text])
if(dir_exists==0):
    subprocess.call(["hadoop", "fs", "-rm", "-r","-skipTrash",df_time_loc_text])
dir_exists=subprocess.call(["hadoop", "fs", "-test", "-d", df_model_performance_loc_text])
if(dir_exists==0):
    subprocess.call(["hadoop", "fs", "-rm", "-r","-skipTrash",df_model_performance_loc_text])
dir_exists=subprocess.call(["hadoop", "fs", "-test", "-d", df_sales_loc_text])
if(dir_exists==0):
    subprocess.call(["hadoop", "fs", "-rm", "-r","-skipTrash",df_sales_loc_text])
dir_exists=subprocess.call(["hadoop", "fs", "-test", "-d", df_opt_loc_text])
if(dir_exists==0):
    subprocess.call(["hadoop", "fs", "-rm", "-r","-skipTrash",df_opt_loc_text])
## combine the data with the header
sc.parallelize([df_time.columns]).union(df_time.rdd).map(lambda p: ','.join(str(item) for item in p)).coalesce(1).saveAsTextFile(df_time_loc_text)
sc.parallelize([df_model_performance.columns]).union(df_model_performance.rdd).map(lambda p: ','.join(str(item) for item in p)).coalesce(1).saveAsTextFile(df_model_performance_loc_text)
sc.parallelize([df_sales.columns]).union(df_sales.rdd).map(lambda p: ','.join(str(item) for item in p)).coalesce(1).saveAsTextFile(df_sales_loc_text)
sc.parallelize([df_opt.columns]).union(df_opt.rdd).map(lambda p: ','.join(str(item) for item in p)).coalesce(1).saveAsTextFile(df_opt_loc_text)

