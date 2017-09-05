## 1. mark off the sc and sqlContext
## 2. change the loc of the input and output paths

# In[1]:

################################################## 0: import modules, create Spark Context and define functions
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.sql import Row
from pyspark.sql import functions as F
from math import floor
from pyspark.sql.functions import col, date_add
import numpy as np
from lpsolve55 import *
import pandas as pd
import time
from datetime import datetime, timedelta
import subprocess
import itertools
import sys

sc = SparkContext()
sqlContext = HiveContext(sc)
start_time = time.time()


# In[2]:

## encoder the stringindexed categorical features to One-Encoder format
def onehot_encoder(df, features_categorical_indexed):
    for s in features_categorical_indexed:
        encoder = OneHotEncoder(dropLast=True, inputCol=s, outputCol=(s + "_Vec"))
        df = encoder.transform(df)
    return df


## convert vec features to format acceptted by Labeledpoint
def convert_sparsevec_to_vec_df(p, index):
    ptmp = [pi.toArray() if i in index else [pi] for i, pi in enumerate(p)]
    return [pii for pi in ptmp for pii in pi]


## return the index of one string vector in another string vector
def columnindex(header, fields):
    return [header.index(s) for s in fields]


## concatenate variables at particular index with string s in p
def concatenate_variables(p, index, s):
    p_tmp = np.array([str(pi) for pi in p])
    return p + [s.join(p_tmp[index])]


## extract elements from list based on index
def extract_from_list_based_on_index(p, index):
    return tuple([p[i] for i in index])


# In[3]:

## scoring function
def scoring_df_batch(df, price_K, competing_group_vars, features_categorical_train_and_test,
                     features_numerical_train_and_test, rfModel):
    features_categorical_indexed_test = [s + "_StringIndexed" for s in features_categorical_train_and_test]
    ##calculate the range of price and price sum
    min_cost_df = df.groupBy(competing_group_vars).agg(F.min(df.cost).alias("min_cost"))
    max_msrp_df = df.groupBy(competing_group_vars).agg(F.max(df.msrp).alias("max_msrp"))
    count_df = df.groupBy(competing_group_vars).count().alias("count")
    price_range_df = min_cost_df.join(max_msrp_df, competing_group_vars, "outer")
    price_range_df = price_range_df.join(count_df, competing_group_vars, "outer").collect()
    price_single_range_df = [list(pr[0:len(competing_group_vars)]) + [pr[-1]] + [i] for pr in price_range_df for i in
                             np.linspace(pr[-3], pr[-2], price_K).tolist()]
    price_single_range_df = sqlContext.createDataFrame(price_single_range_df, competing_group_vars + ["count", "price"])
    price_sum_range_df = [list(pr[0:len(competing_group_vars)]) + [i] for pr in price_range_df for i in
                          np.linspace(pr[-1] * pr[-3], pr[-1] * pr[-2], (price_K - 1) * pr[-1] + 1).tolist()]
    price_sum_range_df = sqlContext.createDataFrame(price_sum_range_df, competing_group_vars + ["price_sum"])
    price_single_sum_range_df = price_single_range_df.join(price_sum_range_df, competing_group_vars, "outer")
    df_price_added = df.join(price_single_sum_range_df, competing_group_vars, "outer")
    ##calculate the relative price, discount
    df_price_added = df_price_added.withColumn("rl_price", (col("price") * col("count") / col("price_sum")))
    df_price_added = df_price_added.withColumn("discount", (col("msrp") - col("price")) / col("msrp"))
    ##one hot encoder for string-indexed categorical features
    df_price_added = onehot_encoder(df_price_added, features_categorical_indexed_test)
    features_categorical_indexed_vec_test = [s + "_StringIndexed_Vec" for s in features_categorical_train_and_test]
    features_modeled_test = features_numerical_train_and_test + features_categorical_indexed_vec_test  ##no label, only features
    features_categorical_indexed_vec_index_test = columnindex(features_modeled_test,
                                                              features_categorical_indexed_vec_test)
    ## select the one-hot-encoded categorical features along with numerical features as well as label to contrust the modeling dataset
    ## make predictions for test_data
    index_temp = columnindex(df_price_added.columns, features_modeled_test)
    df_price_added_rdd = df_price_added.rdd.map(list)
    predictions = rfModel.predict(df_price_added_rdd.map(lambda p: [p[i] for i in index_temp]).map(
        lambda p: convert_sparsevec_to_vec_df(p, features_categorical_indexed_vec_index_test)))
    df_for_output = df_price_added_rdd.zip(predictions).map(lambda p: [p[0], max(round(p[1]), 0)])
    return df_for_output, df_price_added.columns


# In[4]:

## reduce_key_vars: ['week_start', 'department_id', 'store_id', 'price_sum']
## reduce_value_vars: ['product_id','price','cost','msrp']
## input p (df_batch_scored_names,pred_demand) ==> ((competing_group_vars+'price_sum'),['product_id','price','obj','cost','msrp'])
def reduce_key_value_map_LPk(p, index_reduce_key_vars, index_reduce_value_vars):
    key = extract_from_list_based_on_index(p[0], index_reduce_key_vars)
    value = extract_from_list_based_on_index(p[0], index_reduce_value_vars)
    product_id, price, cost, msrp = value
    sales = max(0, round(p[1]))
    obj = (price - cost) * sales
    return (key, [[product_id, price, obj, cost, msrp]])


## Linear Programming Optimization function
def LPk(p_tmp):
    instance_reduce_key = p_tmp[0]
    instance_reduce_value = p_tmp[1]
    instance_reduce_value = pd.DataFrame(instance_reduce_value, columns=['product_id', 'price', 'obj', 'cost', 'msrp'])
    instance_reduce_value = instance_reduce_value.sort_values(by=['product_id', 'price'], ascending=[True, True])
    obj_list = instance_reduce_value['obj'].tolist()
    products_num = len(instance_reduce_value['product_id'].unique())
    price_num = len(instance_reduce_value['price'].unique())
    ## get whether_valid_price, if equal to 1, valid, if equal to 0, invalid
    ## invalid ones needed to set into the constrains
    instance_reduce_value['whether_valid_price'] = instance_reduce_value.apply(
        lambda row: 1 if row['price'] >= row['cost'] and row['price'] <= row['msrp'] else 0, axis=1)
    for i in range(products_num):
        whether_valid_price_individual_product = instance_reduce_value['whether_valid_price'].iloc[
                                                 price_num * i:price_num * (i + 1) - 1]
        if all(whether_valid_price == 0 for whether_valid_price in whether_valid_price_individual_product):
            abs_diff_price = abs(
                instance_reduce_value['msrp'].iloc[price_num * i:price_num * (i + 1)] - instance_reduce_value[
                                                                                            'price'].iloc[
                                                                                        price_num * i:price_num * (
                                                                                        i + 1)])
            row_index = i * price_num + min(enumerate(abs_diff_price), key=lambda x: x[1])[0]
            instance_reduce_value.iloc[row_index, instance_reduce_value.columns.get_loc('whether_valid_price')] = 1
    lp = lpsolve('make_lp', 0, len(instance_reduce_value))
    ret = lpsolve('set_verbose', lp, IMPORTANT)
    ## set objective function
    ret = lpsolve('set_obj_fn', lp, obj_list)
    ## set constrains 1
    for i in range(products_num):
        cons_tmp = [1 if (i_tmp >= price_num * i) & (i_tmp < price_num * (i + 1)) else 0 for i_tmp in
                    range(len(instance_reduce_value))]
        ret = lpsolve('add_constraint', lp, cons_tmp, EQ, 1)
    ## set constrains 2
    price_list_unique = sorted(instance_reduce_value['price'].unique())
    price_list_tmp = []
    for i in range(products_num):
        price_list_tmp += price_list_unique
    ret = lpsolve('add_constraint', lp, price_list_tmp, EQ, instance_reduce_key[3])
    ## add constrains about the invalid price: price larger than msrp or price smaller than cost
    instance_reduce_value_whether_valid_price = instance_reduce_value['whether_valid_price'].tolist()
    for i in range(len(instance_reduce_value_whether_valid_price)):
        if instance_reduce_value_whether_valid_price[i] == 0:
            cons_valid_tmp = [1 if ii == i else 0 for ii in range(len(instance_reduce_value_whether_valid_price))]
            lpsolve('add_constraint', lp, cons_valid_tmp, EQ, 0)
    ## set lower bound and upper bound
    ret = lpsolve('set_lowbo', lp, [0 for i in range(len(instance_reduce_value))])
    ret = lpsolve('set_upbo', lp, [1 for i in range(len(instance_reduce_value))])
    ret = lpsolve('set_maxim', lp)
    ret = lpsolve('solve', lp)
    obj_val = lpsolve('get_objective', lp)
    ret = lpsolve('delete_lp', lp)
    obj_nested_list = []
    for i in range(products_num):
        obj_nested_list.append(obj_list[(price_num * i):(price_num * (i + 1))])
    LBk = obj_val - max([max(item) - min(item) for item in obj_nested_list])
    (week_start, department_id, store_id, price_sum) = instance_reduce_key
    return ((week_start, department_id, store_id), [[price_sum, LBk, obj_val, p_tmp[1]]])


# In[5]:

## Integer Programming Optimization function for IPk
## ps_tmp: [price_sum,LBk,obj_val,p_tmp[1]]
## p_tmp[1]: [[product_id,price,obj,cost,msrp]]
def IPk(ps_tmp):
    obj_mat = ps_tmp[3]
    obj_mat = pd.DataFrame(obj_mat, columns=['product_id', 'price', 'obj', 'cost', 'msrp'])
    obj_mat = obj_mat.sort_values(by=['product_id', 'price'], ascending=[True, True])
    obj_list = obj_mat['obj'].tolist()
    products_num = len(obj_mat['product_id'].unique())
    price_num = len(obj_mat['price'].unique())
    ## get whether_valid_price, if equal to 1, valid, if equal to 0, invalid
    ## invalid ones needed to set into the constrains
    obj_mat['whether_valid_price'] = obj_mat.apply(
        lambda row: 1 if row['price'] >= row['cost'] and row['price'] <= row['msrp'] else 0, axis=1)
    for i in range(products_num):
        whether_valid_price_individual_product = obj_mat['whether_valid_price'].iloc[
                                                 price_num * i:price_num * (i + 1) - 1]
        if all(whether_valid_price == 0 for whether_valid_price in whether_valid_price_individual_product):
            abs_diff_price = abs(obj_mat['msrp'].iloc[price_num * i:price_num * (i + 1)] - obj_mat['price'].iloc[
                                                                                           price_num * i:price_num * (
                                                                                           i + 1)])
            row_index = i * price_num + min(enumerate(abs_diff_price), key=lambda x: x[1])[0]
            obj_mat.iloc[row_index, obj_mat.columns.get_loc('whether_valid_price')] = 1
    lp = lpsolve('make_lp', 0, len(obj_mat))
    ret = lpsolve('set_verbose', lp, IMPORTANT)
    ## set objective function
    ret = lpsolve('set_obj_fn', lp, obj_list)
    ## set constrains 1
    for i in range(products_num):
        cons_tmp = [1 if (i_tmp >= price_num * i) & (i_tmp < price_num * (i + 1)) else 0 for i_tmp in
                    range(len(obj_mat))]
        ret = lpsolve('add_constraint', lp, cons_tmp, EQ, 1)
    ## set constrains 2
    price_list_unique = sorted(obj_mat['price'].unique())
    price_list_tmp = []
    for i in range(products_num):
        price_list_tmp += price_list_unique
    ret = lpsolve('add_constraint', lp, price_list_tmp, EQ, ps_tmp[0])
    ## add constrains about the invalid price: price larger than msrp or price smaller than cost
    obj_mat_whether_valid_price = obj_mat['whether_valid_price'].tolist()
    for i in range(len(obj_mat_whether_valid_price)):
        if obj_mat_whether_valid_price[i] == 0:
            cons_valid_tmp = [1 if ii == i else 0 for ii in range(len(obj_mat_whether_valid_price))]
            lpsolve('add_constraint', lp, cons_valid_tmp, EQ, 0)
    ## set lower bound and upper bound
    ret = lpsolve('set_lowbo', lp, [0 for i in range(len(obj_mat))])
    ret = lpsolve('set_upbo', lp, [1 for i in range(len(obj_mat))])
    ## set integer
    ret = lpsolve('set_int', lp, [1 for i in range(len(obj_mat))])
    ret = lpsolve('set_maxim', lp)
    ret = lpsolve('solve', lp)
    obj_val = lpsolve('get_objective', lp)
    obj_var = lpsolve('get_variables', lp)[0]
    ret = lpsolve('delete_lp', lp)
    obj_mat['obj_var'] = obj_var
    obj_price = obj_mat[obj_mat['obj_var'] == 1][['product_id', 'price']].values.tolist()
    return [obj_val, obj_price]


# In[6]:

##########################IPk for competing group
def IPk_for_competing_group(p_tmp):
    ###input p_tmp
    ## p_tmp: ((week_start,department_id,store_id), instance_reduce_value)
    ## instance_reduce_value: [[price_sum,LBk,obj_val,p_tmp[1]]]
    ## p_tmp[1]: [[product_id,price,obj,cost,msrp]]
    instance_reduce_key = p_tmp[0]
    instance_reduce_value = p_tmp[1]
    instance_reduce_value.sort(key=lambda x: x[2], reverse=True)
    LB = max([item[1] for item in instance_reduce_value])
    for i, ps_tmp in enumerate(instance_reduce_value):
        [obj_val, obj_price] = IPk(ps_tmp)
        if i == 0:
            opt_index = 0
            opt_price = obj_price
        if obj_val > LB:
            LB = obj_val
            opt_price = obj_price
            opt_index = i
        if i == len(instance_reduce_value) - 1:
            break
        if LB >= instance_reduce_value[i + 1][2]:
            break
    opt_solution = [list(instance_reduce_key) + [instance_reduce_value[opt_index][0]] + item for item in opt_price]
    return opt_solution


# In[7]:

## create filter condition for scored batch data to get the predicted deamnd based on optimal price
def create_filter_condition(p):
    p_tmp = [p[0][i] for i in columnindex(df_batch_scored_names, df_batch_scored_reduced_names)]
    return ('_'.join([str(item) for item in p_tmp]), p[0] + [p[1]])


# In[8]:

################################################## 1: define paths of input files and output files
## adl name
adl_name = sys.argv[1]
## blob storage name
#storage_name = sys.argv[2]
## default container name for blob storage
#default_container_name = sys.argv[1]
## adl link
adl_loc = "adl://" + adl_name + ".azuredatalakestore.net/"
## input paths
df_train_loc = adl_loc + "train_data/"
df_sales_loc = adl_loc + "aggregated_sales_data/"
processed_time_d_loc = adl_loc + "publicparameters/processed_time_df.csv"
modelDir = adl_loc + "Models/"
## output paths
# df_time_loc=adl_loc+"powerbi_data/time_data/"
price_change_d_loc = adl_loc + "medium_results/suggested_prices"
opt_results_d_loc = adl_loc + "opt_results_data/"
#df_time_hive_loc = "wasb://" + storage_name + "@" + default_container_name + ".blob.core.windows.net/hive/warehouse/df_time"

# In[9]:

################################################## 2: read into input data and construct df_test
## read into train data and sales data
df_train = sqlContext.read.parquet(df_train_loc)
df_sales = sqlContext.read.parquet(df_sales_loc)
## get the start date and end date of the current sales cycle
processed_time_d_file = sc.textFile(processed_time_d_loc).collect()
processed_time_d = [datetime.strptime(s, '%Y-%m-%d').date() for s in processed_time_d_file[1].split(',')]

# In[10]:

## construct the df_test
## construct the df_test based on the lastest week's data
df_sales_date_max = processed_time_d[1] - timedelta(7)
df_test = df_sales.filter(df_sales.week_start == df_sales_date_max).withColumnRenamed('week_start', 'week_start_origin')
## how many weeks to predict ahead: num_weeks_ahead
## note: if using with the current simulator, it is a must to keep num_weeks_ahead=0, because there are some dependencies with the current version of simulator
## note: if working with the real data, it is fine to change this parameter, to provide suggested price several weeks ahead the next week
num_weeks_ahead = 0
df_test = df_test.withColumn('week_start', date_add(col('week_start_origin'), 7 * (num_weeks_ahead + 1)))
## only do optimization for stores in the treatment group
df_test = df_test.filter(col('group_val') == 'treatment').drop('group_val')
df_test = df_test.select(
    ['store_id', 'product_id', 'department_id', 'brand_id', 'msrp', 'cost', 'avg_hhi', 'avg_traffic',
     'week_start']).distinct()

# In[11]:

################################################## 3: load the model built last time
dirfilename_load = sqlContext.read.parquet(modelDir + "model_name").map(lambda p: p).filter(
    lambda p: p[0] == "model_name").map(lambda p: p[1]).collect()
dirfilename_load = dirfilename_load[0]
rfModel = RandomForestModel.load(sc, dirfilename_load)

# In[12]:

################################################## 4: optimization
##define categorical features, which used in modeling
features_categorical_train_and_test = ["department_id", "brand_id"]
features_numerical_train_and_test = ["price", "avg_hhi", "avg_traffic", "rl_price", "discount"]
##feature index the categorical features
for s in features_categorical_train_and_test:
    s_StringIndexed = s + "_StringIndexed"
    indexer = StringIndexer(inputCol=s, outputCol=s_StringIndexed)
    indexer_trans = indexer.fit(df_train)
    df_test = indexer_trans.transform(df_test)
## step 4.1: input for whole prize optimization
## input 1: df (Spark DataFrame) here: M*CP
## input 2: competing_group_vars
df = df_test
competing_group_vars = ['week_start', 'department_id', 'store_id']
## add an index number for competing groups
df_names = df.columns
competing_group_index_column_name = "competing_group_index"
row_with_index = Row(*(df.columns + [competing_group_index_column_name]))
schema = StructType(df.schema.fields[:] + [StructField(competing_group_index_column_name, StringType(), False)])
concatenate_index = columnindex(df.columns, competing_group_vars)
df = df.rdd.map(lambda p: row_with_index(*concatenate_variables(list(p), concatenate_index, '_'))).toDF(schema)
competing_group_index_column_name_StringIndexed = competing_group_index_column_name + "_StringIndexed"
indexer = StringIndexer(inputCol=competing_group_index_column_name,
                        outputCol=competing_group_index_column_name_StringIndexed)
df = indexer.fit(df).transform(df)
df.cache()  ##need to unpersist
## step 4.2: input for optimization for each batch
## tuning parameter 1: batch_size
## tuning parameter 2: price_K
## input 1: df (Spark DataFrame): M*CP
## other inputs: competing_group_index_column_name_StringIndexed
competing_group_count = int(list(df.agg({competing_group_index_column_name_StringIndexed: "max"}).collect())[0][0]) + 1
batch_size = 500
price_K = 10
batch_size_index_starts = range(0, competing_group_count, batch_size) + [competing_group_count]

# In[13]:

## for each batch
## iterate index -- batch_size_index: 0:(len(batch_size_index_starts)-1)
df_final_output = []
for batch_size_index in range(len(batch_size_index_starts) - 1):
    ## step 4.2.1 calculate the scoring data for this batch
    ## calculate df_batch: M*batch_size
    df_batch = df.filter(
        competing_group_index_column_name_StringIndexed + ">=" + str(batch_size_index_starts[batch_size_index])).filter(
        competing_group_index_column_name_StringIndexed + "<" + str(batch_size_index_starts[batch_size_index + 1]))
    df_batch_scored, df_batch_scored_names = scoring_df_batch(df_batch, price_K, competing_group_vars,
                                                              features_categorical_train_and_test,
                                                              features_numerical_train_and_test, rfModel)
    df_batch_scored.cache()
    ## 4.2.2: reduce the df_batch_scored to the competing group level
    ## calculate the objective function: (price-cost)*pred_demand
    reduce_value_vars = ['product_id', 'price', 'cost', 'msrp']
    index_reduce_key_vars = columnindex(df_batch_scored_names, competing_group_vars + ['price_sum'])
    index_reduce_value_vars = columnindex(df_batch_scored_names, reduce_value_vars)

    ## reducebyKey: competing_vars + ['price_sum','product_id','price']
    df_batch_scored_reduced = df_batch_scored.map(
        lambda p: reduce_key_value_map_LPk(p, index_reduce_key_vars, index_reduce_value_vars)).reduceByKey(
        lambda a, b: a + b)
    ## 4.2.3 parallel optimization for LPk
    ## input optimization_list
    ## parallel optimization for LPk
    ## input p ((competing_group_vars+'price_sum'),['product_id','price','obj']) ==> ((competing_group_vars), ['price_sum','LBk',['product_id','price','obj']])
    df_batch_scored_reduced = df_batch_scored_reduced.map(LPk)
    ## 4.2.4 parallel optimization for IPk
    ## reduce by (competing_group_vars)
    df_batch_scored_reduced = df_batch_scored_reduced.reduceByKey(lambda a, b: a + b).flatMap(IPk_for_competing_group)
    df_batch_scored_reduced_names = ['week_start', 'department_id', 'store_id', 'price_sum', 'product_id', 'price']
    ## 4.2.5 get the demand forecasting for optimal price choices
    df_batch_scored_reduced_filter_condition = df_batch_scored_reduced.map(
        lambda p: '_'.join([str(item) for item in p])).collect()
    df_batch_scored_pred_demand = df_batch_scored.map(create_filter_condition).filter(
        lambda p: p[0] in df_batch_scored_reduced_filter_condition).map(lambda p: p[1])
    df_batch_scored_pred_demand_names = df_batch_scored_names + ['pred_demand']
    ## exclude those intermediate variables created for mllib modeling
    df_batch_scored_pred_demand_names_needed_names = [item for i, item in enumerate(df_batch_scored_pred_demand_names)
                                                      if 'StringIndexed' not in item]
    df_batch_scored_pred_demand_names_needed_index = [i for i, item in enumerate(df_batch_scored_pred_demand_names) if
                                                      'StringIndexed' not in item]
    df_batch_scored_pred_demand_needed = df_batch_scored_pred_demand.map(
        lambda p: [p[i] for i in df_batch_scored_pred_demand_names_needed_index]).collect()
    df_final_output = df_final_output + df_batch_scored_pred_demand_needed
    df_batch_scored.unpersist()

# In[14]:

################################################## 4: output the data
dir_exists = subprocess.call(
    ["hadoop", "fs", "-test", "-d", opt_results_d_loc + 'week_start=' + processed_time_d[1].strftime('%Y-%m-%d')])
if dir_exists == 1:
    ## output the suggested_prices.csv
    price_change_names = ['product_id', 'store_id', 'week_start', 'price']
    price_change_names_index = columnindex(df_batch_scored_pred_demand_names_needed_names, price_change_names)
    price_change_d = sc.parallelize([[p[item].strftime(
        '%Y-%m-%d %H:%M:%S') if item == df_batch_scored_pred_demand_names_needed_names.index('week_start') else p[item]
                                      for i, item in enumerate(price_change_names_index)] for p in df_final_output])
    price_change_d = price_change_d.map(lambda p: ','.join(str(item) for item in p))
    price_change_d_loc_with_date=price_change_d_loc+'_'+processed_time_d[0].strftime('%Y_%m_%d')
    dir_exists = subprocess.call(["hadoop", "fs", "-test", "-d", price_change_d_loc_with_date])
    if (dir_exists == 0):
        subprocess.call(["hadoop", "fs", "-rm", "-r", "-skipTrash", price_change_d_loc_with_date])
    price_change_d.repartition(1).saveAsTextFile(price_change_d_loc_with_date)
    ## output the optimization results
    df_opt = sqlContext.createDataFrame(sc.parallelize(df_final_output), df_batch_scored_pred_demand_names_needed_names)
    df_opt.write.partitionBy('week_start').parquet(opt_results_d_loc, mode='append')
    ## for Power BI
    df_opt.write.saveAsTable("df_opt", format="parquet", mode="append", partitionBy='week_start')

    ## output the time
    end_time = time.time()
    agg_time = processed_time_d[1]
    df_time = sqlContext.createDataFrame(
        sc.parallelize([["optimization", (end_time - start_time), str(agg_time - timedelta(7))]]), StructType(
            [StructField("step", StringType(), True), StructField("time", DoubleType(), True),
             StructField("exec_date", StringType(), True)]))
    # df_time.repartition(1).write.parquet(df_time_loc,mode='append')
    ## for Power BI
    df_time.write.saveAsTable("df_time", format="parquet", mode="append")
    #df_time = sqlContext.read.parquet(df_time_hive_loc)
    df_time = sqlContext.sql("select * from df_time")
    ################################################## 3: deal with the df_time
    ## make the model training zero when model not trained
    exec_date_list = df_time.select('exec_date').distinct().collect()
    exec_date_list = [item.exec_date for item in exec_date_list]
    step_list = df_time.select('step').distinct().collect()
    step_list = [item.step for item in step_list]
    df_time_all = sqlContext.createDataFrame(
        sc.parallelize([list(element) for element in itertools.product(exec_date_list, step_list)]),
        StructType([StructField('exec_date', StringType(), True), StructField('step', StringType(), True)]))
    df_time = df_time.join(df_time_all, ['step', 'exec_date'], 'outer')
    df_time = df_time.fillna({'time': 0})
    df_time.write.saveAsTable("df_time_final", format="parquet", mode="overwrite")
df.unpersist()
