#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 10:21:53 2019

@author: rafaelhernandez
"""

import findspark
findspark.init()
import pyspark as ps
import warnings
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *


try:
    # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
    conf = SparkConf().setAll([('spark.executor.memory', '8g'),('spark.driver.memory','8g')]) 
    sc = ps.SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    print("Just created a SparkContext")
except ValueError:
    warnings.warn("SparkContext already exists in this scope")
    
sc.master

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/train_sentiment.csv')

test_df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/Biden.csv')

type(df)

df.show(5)

df = df.dropna()

df.count()

df = df.withColumn("label", df["label"].cast(IntegerType()))

df.dtypes

(train_set, val_set, test_set) = df.randomSplit([0.98, 0.01, 0.01], seed = 2000)

#HashingTF + IDF + Logistic Regression

from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

tokenizer = Tokenizer(inputCol="text", outputCol="words")
cv = CountVectorizer(vocabSize=2**16, inputCol="words", outputCol='cv')
idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "label", outputCol = "label")
rf = RandomForestClassifier()

#df = df.withColumn("label", df["label"].cast(IntegerType()))
train_set = train_set.dropna()

pipeline = Pipeline(stages=[tokenizer, cv, idf, rf])

pipelineFit = pipeline.fit(train_set)
rf_predictions = pipelineFit.transform(test_df)

rf_predictions.show()

pipeline.write().overwrite().save('pipelineFit')

pipeline = PipelineModel.load('pipelineFit')

#pipelineFit2 = PipelineModel.load("/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/pipelineFit")

#evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
#evaluator.evaluate(lr_predictions)

#rf = RandomForestClassifier(maxIter=100)
#rfModel = rf.fit(train_df)
#rf_predictions = rfModel.transform(val_df)
#train_df.show(5)

#evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
#evaluator.evaluate(rf_predictions)