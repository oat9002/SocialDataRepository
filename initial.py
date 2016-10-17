from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys

spark = SparkSession\
    .builder\
    .appName("Initial TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

#for tweetQuery
try:
    tweetQueryDF = spark.read.json("tweetQuery.json")
    tweetQueryDF.write.parquet("tweetQuery.parquet")
except:
    print(sys.exc_info()[1])

#for tweet
try:
    tweetDF = spark.read.json("tweet.json")
    tweetDF.write.parquet("tweet.parquet")
except:
    print(sys.exc_info()[1])
