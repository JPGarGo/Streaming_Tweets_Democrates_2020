import findspark
findspark.init()
import pyspark as ps
import warnings
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.ml import PipelineModel, Pipeline

try:
    # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
    conf = SparkConf().setAll([('spark.executor.memory', '8g'),('spark.driver.memory','8g')]) 
    sc = ps.SparkContext()
    sqlContext = SQLContext(sc)
    print("Just created a SparkContext")
except ValueError:
    warnings.warn("SparkContext already exists in this scope")
    
sc.master


df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/Biden.csv')

type(df)

df.show(5)

pipeline = PipelineModel.load("/Users/rafaelhernandez/Documents/GitHub/Streaming_Tweets_Democrats_2020/pipelineFit")

predictions = pipeline.transform(df)