# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(__file__) + '../repository')
sys.path.append(os.path.dirname(__file__) + '../service')
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from repository import TwitterRepository
# from repository import SocialDataRepository
import os
import json
import googlemaps
from geopy.distance import great_circle
from decimal import Decimal
import pydoop.hdfs 


spark = SparkSession\
    .builder\
    .appName("SocialDataRepository")\
    .getOrCreate()
    # .master("spark://stack-02:7077")\

sc = spark.sparkContext
hdfs = pydoop.hdfs.hdfs()
if hdfs.exists("hdfs://stack-02:9000/SocialDataRepository/TW_TWEET.parquet"):
    print(True)
else:
    print(False)
# queryParquet = "./QUERY.parquet"
# queryBaseDF = spark.read.parquet(queryParquet)
# queryBaseDF.show()
# #
# queryDF = spark.read.parquet("../QUERY.parquet")
# keywords = queryDF.select(queryDF.id, queryDF.keyword).collect()
# print keywords[0]['id']
#
# tweetDF = spark.read.parquet("TW_TWEET.parquet")
# print tweetDF.toJSON().first()
# print tweetDF.count()

# path = os.path.join(os.path.dirname(__file__), '../tweet.json')
# print(path)
# with open(path) as json_data:
#     tweetJson = json.load(json_data)
#     TwitterRepository.saveRawTweet(tweetJson)
# with open("./SocialDataRepository/TW_TWEET_BACKUP.json", 'r') as test:
#     json = [json.loads(line) for line in test]
#     print("total backup: ", len(json))
#     print(json[len(json) - 1])
# with open(path, 'r') as test:
#     json = json.load(test)
#     print(len(json))
# df = spark.read.json(os.path.dirname(__file__), '../../tweet_backup.json')
# print(df.count())

# queryDF = spark.read.parquet("./SocialDataRepository/QUERY.parquet")
# placeDF = spark.read.parquet("./QUERY.parquet")
# print(placeDF.count())
# newPlace = {}
# newPlace['keyword'] = "paradise park"
# SocialDataRepository.addPlaceOrQuery(newPlace)
# placeDF = spark.read.parquet("./QUERY.parquet")
# print(placeDF.collect())
# txt = 'สยามพารากอน'
# placeDF = placeDF.where(placeDF.name != txt)
# placeDF.show()
# placeDF.write.mode("overwrite").parquet("./PLACE.parquet")
# test = placeDF.where()
# placeDF.show()
# test = queryDF.join(placeDF, queryDF.place_id == placeDF.id, 'outer')
#
# queryDF.show()
# placeDF.show()
# print "query: " + test.keyword + " place: " + test.name

# gmaps = googlemaps.Client(key='AIzaSyA0_9hFyqLO5uV5pWQUSGL0g5MmtsXwNj4')
# places = gmaps.places(query="kmitl")
# print places['results']
# for place in places['results']:
#     place = json.loads(json.dumps(place, ensure_ascii=False))
#     print SocialDataRepository.comparePlace(place['geometry']['location']['lat'], place['geometry']['location']['lng'], 13.734760, 100.777690)

#test sort Twitter
# path = os.path.join(os.path.dirname(__file__), '../tweet.json')
# print(path)
# with open(path) as json_data:
#     json = json.load(json_data)
# tweetDF = spark.read.parquet("../TW_TWEET.parquet")
# test = tweetDF.sort(tweetDF.created_at.desc()).limit(10)
# test.show()
