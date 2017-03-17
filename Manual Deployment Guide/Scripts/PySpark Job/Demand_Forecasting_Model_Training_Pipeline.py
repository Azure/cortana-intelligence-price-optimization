# In[1]:

################################################## 0: import modules, create Spark Context and define functions
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.ml.feature import OneHotEncoder, StringIndexer, StandardScaler
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.sql.functions import countDistinct, col
from pyspark.sql import functions as F
import numpy as np
from numpy import array
from datetime import datetime, timedelta
import time
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


## return the index of one string vector in another string vector
def columnindex(header, fields):
    return [header.index(s) for s in fields]


## convert vec features to format acceptted by Labeledpoint
def convert_sparsevec_to_vec_df(p, index):
    ptmp = [pi.toArray() if i in index else [pi] for i, pi in enumerate(p)]
    return [pii for pi in ptmp for pii in pi]


# In[3]:

################################################## 1: define paths of input files and output files
## adl Name
adl_name = sys.argv[1]
## adl link
adl_loc = "adl://" + adl_name + ".azuredatalakestore.net/"
## input files
df_sales_loc = adl_loc + "aggregated_sales_data/"
processed_time_d_loc = adl_loc + "publicparameters/processed_time_df.csv"
## output files
df_train_loc = adl_loc + "train_data/"
# df_time_loc=adl_loc+"powerbi_data/time_data/"
modelDir = adl_loc + "Models/"
# df_model_performance_loc=adl_loc+"powerbi_data/model_performance_data/"


# In[4]:

## test whether the model exists or not
try:
    dirfilename_load = sqlContext.read.parquet(modelDir + "model_name").map(lambda p: p).filter(
        lambda p: p[0] == "model_name").map(lambda p: p[1]).collect()
    model_exist = True
except:
    model_exist = False

# In[5]:

## if model_exist, means the pipeline for the first has finished, means the training data are ready for retraining
if model_exist:
    ################################################## 2: read into aggregated sales data
    df_sales = sqlContext.read.parquet(df_sales_loc)
    df_sales=df_sales.cache()
    ## get the time the model is built
    model_time = df_sales.agg(F.max(col('week_start'))).collect()
    ################################################## 3: feature engineering: build the features
    ## calculate relative price and discount for train data
    competing_group = ['week_start', 'store_id', 'department_id']
    df_train_price_sum = df_sales.groupBy(competing_group).agg(F.sum(df_sales.price).alias("price_sum"))
    df_train_price_count = df_sales.groupBy(competing_group).count().alias("count")
    df_train = df_sales.join(df_train_price_sum, competing_group, "inner").join(df_train_price_count, competing_group,
                                                                                "inner")
    df_train = df_train.withColumn("rl_price", col("price") * col("count") / col("price_sum")).withColumn("discount", (
    col("msrp") - col("price")) / col("msrp"))
    df_train = df_train.drop("price_sum").drop("count")
    ################################################## 4: prepare the train data for modeling
    ## define categorical features, numerical features as well as label, which used in modeling
    features_categorical_train = ["department_id", "brand_id"]
    features_numerical_train = ["price", "avg_hhi", "avg_traffic", "rl_price", "discount"]
    label_train = ["sales"]
    ## feature index the categorical features
    for s in features_categorical_train:
        s_StringIndexed = s + "_StringIndexed"
        indexer = StringIndexer(inputCol=s, outputCol=s_StringIndexed)
        indexer_trans = indexer.fit(df_train)
        df_train = indexer_trans.transform(df_train)
    features_categorical_indexed_train = [s + "_StringIndexed" for s in features_categorical_train]
    ## one hot encoder for string-indexed categorical features
    df_train = onehot_encoder(df_train, features_categorical_indexed_train)
    features_categorical_indexed_vec_train = [s + "_StringIndexed_Vec" for s in features_categorical_train]
    features_modeled_train = label_train + features_numerical_train + features_categorical_indexed_vec_train
    features_categorical_indexed_vec_index_train = columnindex(features_modeled_train,
                                                               features_categorical_indexed_vec_train)
    ## select the one-hot-encoded categorical features along with numerical features as well as label to contrust the modeling dataset
    df_train_modeling = df_train.select(features_modeled_train)
    ## df_train_modeling_rdd for mllib package
    df_train_modeling_rdd = df_train_modeling.map(
        lambda p: convert_sparsevec_to_vec_df(p, features_categorical_indexed_vec_index_train))
    df_train_modeling_rdd = df_train_modeling_rdd.map(lambda l: LabeledPoint(l[0], l[1:]))
    ################################################## 5: train random forest regression model
    ## random forest
    ## train model
    rfModel = RandomForest.trainRegressor(df_train_modeling_rdd, categoricalFeaturesInfo={}, numTrees=100,
                                          featureSubsetStrategy="auto", impurity='variance', maxDepth=10, maxBins=32)
    # Predict on train data
    predictions = rfModel.predict(df_train_modeling_rdd.map(lambda l: l.features))
    ## Evaluation of the model
    predictionAndObservations = predictions.zip(df_train_modeling_rdd.map(lambda l: l.label))
    testMetrics = RegressionMetrics(predictionAndObservations)
    model_time = str(model_time[0][0])
    df_model_performance = sqlContext.createDataFrame(
        sc.parallelize([[model_time, testMetrics.rootMeanSquaredError, testMetrics.r2]]), ["model_time", "RMSE", "R2"])
    # df_model_performance.repartition(1).write.parquet(df_model_performance_loc,mode='append')
    ## for Power BI
    df_model_performance.write.saveAsTable("df_model_performance", format="parquet", mode="append")

    ################################################## 6: save model
    ## save model
    datestamp = unicode(datetime.now()).replace(' ', '_').replace(':', '_');
    dirfilename = modelDir + "RandomForestRegression" + datestamp
    rfModel.save(sc, dirfilename)
    ## save Model file path information
    model_name = [('model_name', dirfilename)]
    model_name = sqlContext.createDataFrame(model_name)
    model_name.repartition(1).write.mode("overwrite").parquet(modelDir + "model_name")
    ## save the train data
    df_train.write.parquet(df_train_loc, mode='overwrite')
    ## save the time file
    end_time = time.time()
    df_time = sqlContext.createDataFrame(sc.parallelize([["model_training", (end_time - start_time), model_time]]),
                                         StructType([StructField("step", StringType(), True),
                                                     StructField("time", DoubleType(), True),
                                                     StructField("exec_date", StringType(), True)]))
    # df_time.repartition(1).write.parquet(df_time_loc,mode='append')
    ## for Power BI
    df_time.write.saveAsTable("df_time", format="parquet", mode="append")
    ## unpersist cached dataframes
    df_sales.unpersist()

