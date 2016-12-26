# -*- coding: utf-8 -*-
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext
from repository import FoursquareRepository
from repository import SocialDataRepository
from pandas.io.json import json_normalize
import dateutil.parser as date
import json
import os.path as path

spark = SparkSession\
    .builder\
    .appName("Initial FoursquareRepository")\
    .getOrCreate()

sc = spark.sparkContext

with open('fqVenue.json') as json_data:
    venueJSON = json.load(json_data)
    FoursquareRepository.saveVenue(venueJSON['response']['venue'], 12453)

    parquetFile = spark.read.parquet("FQ_VENUE.parquet")
    parquetFile.show()
    # parquetFile.createOrReplaceTempView("parquetFile")
    # x = spark.sql("SELECT * FROM parquetFile")
    # x.show()

# # for tweet
# if not path.exists("TW_TWEET.parquet"):
#     with open('tweet.json') as json_data:
#         tweetsJSON = json.load(json_data)
#         for index in range(0, len(tweetsJSON)):
#             tweetsJSON[index]['created_at'] = tweetsJSON[index]['created_at']['$date']
#             del tweetsJSON[index]['_id']

#         # rawTweets = []
#         # for tweet in tweetsJSON:
#         #     normalizedTweet = json_normalize(tweet)
#         #     map(lambda column: normalizedTweet.rename(columns = {column: ''.join(map(lambda t: t.replace(".", "_"), list(column)))}, inplace = True) ,normalizedTweet.columns)
#         #     rawTweets.append(normalizedTweet.to_json())
#         # rawTweetRDD = sc.parallelize(rawTweets)
#         # rawTweetDF = spark.read.json(rawTweetRDD)
#         # rawTweetDF.write.parquet("rawTweet.parquet")

#         tweets = []
#         users = []
#         places = []
#         for tweet in tweetsJSON:
#             queryDF = spark.read.parquet("QUERY.parquet")
#             keywords = queryDF.select(queryDF.id, queryDF.keyword).collect()
#             for keyword in keywords:
#                 if keyword['keyword'] in tweet['text']:
#                     tweets.append(TwitterRepository.selectTweetCol(tweet, keyword['id']))
#             users.append(TwitterRepository.selectUserCol(tweet['user']))
#         tweetRDD = sc.parallelize(tweets)
#         tweetDF = spark.createDataFrame(tweetRDD)
#         tweetDF.printSchema()
#         tweetDF.write.parquet("TW_TWEET.parquet")
#         userRDD = sc.parallelize(users)
#         userDF = spark.createDataFrame(userRDD)
#         if not path.exists("TW_USER.parquet"):
#             userDF.write.parquet('TW_USER.parquet')

# #for query
# if not path.exists("QUERY.parquet"):
#     with open('tweetQuery.json') as json_data:
#         queryJson = json.load(json_data)
#         queries = []
#         for query in queryJson['queries']:
#             queries.append(SocialDataRepository.createQuerySchema(query))
#         queryRDD = sc.parallelize(queries)
#         queryDF = spark.createDataFrame(queryRDD)
#         queryDF.write.parquet("QUERY.parquet")
