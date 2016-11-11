from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

queryParquet = "../QUERY.parquet"
queryBaseDF = spark.read.parquet(queryParquet)
queryBaseDF.show()

queryDF = spark.read.parquet("../QUERY.parquet")
keywords = queryDF.select(queryDF.id, queryDF.keyword).collect()
print keywords[0]['id']

tweetDF = spark.read.parquet("../TW_TWEET.parquet")
print tweetDF.toJSON().first()
print len(tweets)
print tweetDF.count()
