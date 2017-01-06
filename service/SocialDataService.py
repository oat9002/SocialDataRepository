from pyspark.sql import *

spark = SparkSession\
    .builder\
    .appName("Service")\
    .getOrCreate()

sc = spark.sparkContext

def writeParquet(parquetPath, data):
    if len(data) > 0:
        rowRDD = sc.parallelize(data)
        rowDF = spark.createDataFrame(rowRDD)
        rowDF.write.mode("append").parquet(parquetPath)
    else:
        print("no row written")