from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from ..repository import TwitterRepository
import os
import json

spark = SparkSession\
    .builder\
    .appName("TwitterRepository")\
    .getOrCreate()

sc = spark.sparkContext

# queryParquet = "../QUERY.parquet"
# queryBaseDF = spark.read.parquet(queryParquet)
# queryBaseDF.show()
#
# queryDF = spark.read.parquet("../QUERY.parquet")
# keywords = queryDF.select(queryDF.id, queryDF.keyword).collect()
# print keywords[0]['id']
#
# tweetDF = spark.read.parquet("./SocialDataRepository/TW_TWEET.parquet")
# print tweetDF.toJSON().first()
# print tweetDF.count()
path = os.path.join(os.path.dirname(__file__), '../tweet.json')
print(path)
with open(path) as json_data:
    tweetJson = json.load(json_data)
    TwitterRepository.saveRawTweet(tweetJson)
with open("./SocialDataRepository/TW_TWEET_BACKUP.json", 'r') as test:
    json = [json.loads(line) for line in test]
    print("total backup: ", len(json))
    print(json[len(json) - 1])
# with open(path, 'r') as test:
#     json = json.load(test)
#     print(len(json))
# df = spark.read.json(os.path.dirname(__file__), '../../tweet_backup.json')
# print(df.count())
