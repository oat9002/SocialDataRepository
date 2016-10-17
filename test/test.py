from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

twqDF = spark.read.parquet("tweetQuery.parquet")
twqDF.show()
