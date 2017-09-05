
# coding: utf-8

# In[1]:

################################################## 0: import modules, create Spark Context and define functions
import re
from datetime import datetime, timedelta
import subprocess
import sys
import time
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col,udf,lag,date_add,explode
from pyspark.sql.window import Window

from pyspark.mllib.clustering import KMeans, KMeansModel

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
## in spark 2.0: you can access the Spark context through spark.sparkContext, and access the spark through spark
sc = spark.sparkContext
start_time = time.time()


# In[2]:

## eliminate the double quote in the headers
def eliminate_double_quote(header):
    return [s.replace('"','') for s in header]
## convert numeric fields from string in each line of RDD
def convert_string_to_numeric_df(p,index):
    return [str(pi).replace('"','') if i in index else float(pi) for i,pi in enumerate(p)]
## return the index of one string vector in another string vector
def columnindex(header,fields):
    return [header.index(s) for s in fields]
## construct schema of the data frame
def construct_schema(fields_categorical, header):
    return StructType([StructField(field_name, StringType(), True) if field_name in fields_categorical else StructField(field_name, DoubleType(), True) for field_name in header])
## define the stores in treament, control and other group
def define_group(p):
    if p.cluster_val not in valid_groups:
        return list(p)+['other']
    else:
        if p.cum_dist>0.5:
            return list(p)+['control']
        else:
            return list(p)+['treatment']


# In[3]:

################################################## 1: define paths of input files and output files
## blob storage name
blob_storage_name = sys.argv[1]
## container names
raw_data_container = "rawdata"
public_parameters_container = "publicparameters"
result_container = "results"

## input paths
sales_and_pc_raw_d_loc = "wasb://{1}@{0}.blob.core.windows.net/".format(blob_storage_name,raw_data_container)
products_d_loc = "wasb://{1}@{0}.blob.core.windows.net/".format(blob_storage_name,public_parameters_container)+"products.csv"
stores_d_loc = "wasb://{1}@{0}.blob.core.windows.net/".format(blob_storage_name,public_parameters_container)+"stores.csv"
processed_time_d_loc = "wasb://{1}@{0}.blob.core.windows.net/".format(blob_storage_name,public_parameters_container)+"processed_time_df.csv"
## output paths
df_sales_loc = "wasb://{1}@{0}.blob.core.windows.net/".format(blob_storage_name,result_container)+"aggregated_sales_data/"

## input path if exists, output path if not exist
df_stores_loc = "wasb://{1}@{0}.blob.core.windows.net/".format(blob_storage_name,public_parameters_container)+"publicparameters/stores_processed"


# In[4]:

################################################## 2: data aggregation
########################## 2.1 read in products, stores, sales, price_changes data from Data Lake
## get the start date and end date of the current sales cycle
processed_time_d_file = sc.textFile(processed_time_d_loc).collect()
processed_time_d=[datetime.strptime(s, '%Y-%m-%d').date() for s in processed_time_d_file[1].split(',')]


# In[5]:

################# 2.1.1 products data
## import file from blob storage
products_d_file = sc.textFile(products_d_loc)
## define categorical fields and numerical fields
# products data
header_products_original=eliminate_double_quote([str(s) for s in products_d_file.first().split(',')])
header_products=['department_id', 'brand_id', 'product_id', 'msrp', 'cost']
fields_categorical_products=["department_id","brand_id","product_id"]
fields_numerical_products=list(set(header_products)-set(fields_categorical_products))
## turn data into Data Frame
# products data
columnindex_categorical_products=columnindex(header_products,fields_categorical_products)
schema_products=construct_schema(fields_categorical_products, header_products)
df_products_rdd=products_d_file.filter(lambda l: header_products_original[0] not in l).map(lambda p: convert_string_to_numeric_df(p.split(","),columnindex_categorical_products))
df_products=spark.createDataFrame(df_products_rdd,schema_products)


# In[6]:

################# 2.1.2 stores data
## if the store data with group_val already exists, read in
## else: construct the group_val through clustering
try:
    df_stores=spark.read.parquet(df_stores_loc)
    header_stores=['store_id', 'avg_hhi', 'avg_traffic','group_val']
    df_stores_join=df_stores.select(header_stores)
except:
    ## import file from blob storage
    stores_d_file = sc.textFile(stores_d_loc)
    ## define categorical fields and numerical fields
    # store data
    header_stores_original=eliminate_double_quote([str(s) for s in stores_d_file.first().split(',')])
    header_stores=['store_id', 'avg_hhi', 'avg_traffic']
    fields_categorical_stores=["store_id"]
    fields_numerical_stores=list(set(header_stores)-set(fields_categorical_stores))
    ## turn data into Data Frame
    # store data
    columnindex_categorical_stores=columnindex(header_stores,fields_categorical_stores)
    schema_stores=construct_schema(fields_categorical_stores, header_stores)
    df_stores_rdd=stores_d_file.filter(lambda l: header_stores_original[0] not in l).map(lambda p: convert_string_to_numeric_df(p.split(","),columnindex_categorical_stores))
    df_stores=spark.createDataFrame(df_stores_rdd,schema_stores)
    ## split stores into control and treatment group according to store attributes (these attributes should be strongly related to store sales)
    store_group_col_names=['avg_hhi', 'avg_traffic']
    store_group_col_names_std=[col_name+'_std' for col_name in store_group_col_names]
    for col_name in ['avg_hhi', 'avg_traffic']:
        col_name_std=col_name+'_std'
        col_mean=df_stores.agg(F.mean(col(col_name))).collect()[0][0]
        col_std=df_stores.agg(F.stddev(col(col_name))).collect()[0][0]
        standarize_udf=udf(lambda x: (x-col_mean)/col_std,DoubleType())
        df_stores=df_stores.withColumn(col_name_std,standarize_udf(col(col_name)))
    store_group_col_index=columnindex(df_stores.columns,store_group_col_names_std)
    df_stores_rdd=df_stores.rdd.map(list)
    ## perform the clustering to cluster stores according to the attributes
    store_number=df_stores.count()
    if store_number<=2:
        cluster_number=1
    else:
        cluster_number=3
    clusters = KMeans.train(df_stores_rdd.map(lambda p: [p[index] for index in store_group_col_index]), cluster_number,maxIterations=200)
    df_stores_rdd=df_stores_rdd.zip(clusters.predict(df_stores_rdd.map(lambda p: [p[index] for index in store_group_col_index]))).map(lambda p: p[0]+[p[1]])
    df_stores=spark.createDataFrame(df_stores_rdd,StructType(df_stores.schema.fields+[StructField('cluster_val',IntegerType(),True)]))
    df_stores=df_stores.withColumn('rand',F.rand())
    window = Window.partitionBy('cluster_val').orderBy('rand')
    df_stores=df_stores.withColumn('cum_dist',F.cume_dist().over(window))
    ## groups only with larger than 2 stores are valid group to sample from
    valid_groups=df_stores.groupBy(['cluster_val']).count().filter('count>=2').select('cluster_val').collect()
    valid_groups=[i[0] for i in valid_groups]
    df_stores=spark.createDataFrame(df_stores.rdd.map(define_group),StructType(df_stores.schema.fields+[StructField('group_val',StringType(),True)]))
    header_stores=['store_id', 'avg_hhi', 'avg_traffic']
    df_stores.repartition(1).write.parquet(df_stores_loc,mode='overwrite')
    df_stores_join=df_stores.select(header_stores+['group_val'])


# In[7]:

################# 2.1.3 sales data
## parsing sales json file and construct df_sales
sales_dates=[(processed_time_d[0]+timedelta(i+1)).strftime('%Y_%m_%d') for i in range((processed_time_d[1]-processed_time_d[0]).days)]
sales_raw_file_name=sales_and_pc_raw_d_loc+'sales_store[0-9]*_{'+','.join(sales_dates)+'}_00_00_00.json'
sales_jsonRDD=sc.wholeTextFiles(sales_raw_file_name).map(lambda x: x[1])
sales_js = spark.read.json(sales_jsonRDD)
sales_js=sales_js.select(col('SalesLogDateTime'),col('StoreID'),explode(col('Transactions')).alias('Transaction'))
sales_js=sales_js.select(['SalesLogDateTime', 'StoreID', 'Transaction.Products', 'Transaction.Subtotal', 'Transaction.Tax', 'Transaction.Total', 'Transaction.TransactionDateTime'])
sales_js=sales_js.select(col('Subtotal'), col('StoreID'), col('Tax'),col('SalesLogDateTime'), col('Total'), col('TransactionDateTime'),explode(col('Products')).alias('Product'))
sales_js=sales_js.select(['Subtotal', 'StoreID', 'Tax', 'SalesLogDateTime', 'Total', 'TransactionDateTime', 'Product.Price', 'Product.ProductID'])
header_sales_origin=['ProductID','StoreID','TransactionDateTime']
header_sales =['product_id', 'store_id', 'datetime']
df_sales=sales_js.select(header_sales_origin)
for i in range(len(header_sales_origin)):
    df_sales=df_sales.withColumnRenamed(header_sales_origin[i], header_sales[i])
df_sales=df_sales.select(col('product_id'),col('store_id').cast(StringType()).alias('store_id'),col('datetime'))
sales_date_udf=udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date(), DateType())
df_sales=df_sales.withColumn('date_date', sales_date_udf(col('datetime')))


# In[8]:

################# 2.1.4 price changes data
## parsing price change json file and construct df_price_change
price_change_dates=[(processed_time_d[0]+timedelta(i*7)).strftime('%Y_%m_%d') for i in range((processed_time_d[1]-processed_time_d[0]).days/7)]
price_change_raw_file_name=sales_and_pc_raw_d_loc+'pc_store[0-9]*_{'+','.join(price_change_dates)+'}_00_00_00.json'
price_change_jsonRDD=sc.wholeTextFiles(price_change_raw_file_name).map(lambda x: x[1])
price_change_js = spark.read.json(price_change_jsonRDD)
price_change_js=price_change_js.select(col('PriceDate'),col('StoreID'),explode(col('PriceUpdates')).alias('PriceUpdate'))
price_change_js=price_change_js.select(['PriceDate','StoreID','PriceUpdate.Price','PriceUpdate.ProductID'])
header_price_change_origin=['ProductID', 'StoreID', 'PriceDate', 'Price']
header_price_change=['product_id', 'store_id', 'date', 'price']
df_price_change=price_change_js.select(header_price_change_origin)
for i in range(len(header_price_change_origin)):
    df_price_change=df_price_change.withColumnRenamed(header_price_change_origin[i], header_price_change[i])
df_price_change=df_price_change.select(col('product_id'),col('store_id').cast(StringType()).alias('store_id'),col('date'),col('price'))
## deal with the datetime format in df_price_change and df_sales
price_change_date_udf=udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date(), DateType())
df_price_change=df_price_change.withColumn('week_start', price_change_date_udf(col('date')))


# In[9]:

########################## 2.2 aggregate the sales data and join with store and product attributes
## create week_start for df_sales
df_sales_date_min=processed_time_d[0]
sales_week_start_udf=udf(lambda x:df_sales_date_min+timedelta((x-df_sales_date_min).days/7*7),DateType())
df_sales=df_sales.withColumn('week_start',sales_week_start_udf(col('date_date')))
## aggregate df_sales on weekly basis
df_sales=df_sales.groupBy(['week_start','store_id','product_id']).agg({"*": "count"}).withColumnRenamed('count(1)', 'sales')
## join the df_sales with df_price_change, warning: under this scenario, every week each product in each department in each store has an entry in df_price_change
df_sales=df_sales.join(df_price_change,["week_start","store_id","product_id"],'left_outer')
## join df_sales with df_products and df_stores to get products and stores attributes
df_sales=df_sales.join(df_products,["product_id"],"inner").join(df_stores_join,["store_id"],"inner")


# In[10]:

################################################## 3: data export to data lake
df_sales_date_max=processed_time_d[1]-timedelta(7)
dir_exists=subprocess.call(["hadoop", "fs", "-test", "-d", df_sales_loc+'week_start='+df_sales_date_max.strftime('%Y-%m-%d')])
if dir_exists==1:
    df_sales.cache()
    df_sales.write.partitionBy('week_start').parquet(df_sales_loc, mode='append')
    ## for Power BI
    df_sales.write.saveAsTable("df_sales",format="parquet",mode="append",partitionBy='week_start')
    ## for Power BI
    end_time=time.time()
    agg_time=df_sales_date_max
    df_time=spark.createDataFrame(sc.parallelize([["data_clean_and_aggregation",(end_time - start_time),str(agg_time)]]),                                       StructType([StructField("step", StringType(), True),StructField("time", DoubleType(), True),StructField("exec_date", StringType(), True)]))
    #df_time.repartition(1).write.parquet(df_time_loc,mode='append')
    ## for Power BI
    df_time.write.saveAsTable("df_time",format="parquet",mode="append")
    ## for Power BI
    df_sales.unpersist()

