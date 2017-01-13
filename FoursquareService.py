# -*- coding: utf-8 -*-
# import sys
# sys.path.append('../repository')
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext
from repository import FoursquareRepository
from repository import SocialDataRepository
from pandas.io.json import json_normalize
import dateutil.parser as date
import json
import os.path as path
import datetime
import requests

spark = SparkSession\
    .builder\
    .appName("FacebookService")\
    .getOrCreate()

sc = spark.sparkContext

# print(SocialDataRepository.comparePlace("13.72767543677477","13.72767543677477","13.72767543677477","13.72767543677477"))

venueId = "4b0587fdf964a52034ab22e3"

# resp = requests.get('http://localhost:6003/foursquare/detail?venue_id='+venueId)
# if resp.status_code != 200:
#     # This means something went wrong.
#     # raise ApiError('GET /tasks/ {}'.format(resp.status_code))
#     print("Request Error code "+resp.status_code)
# else:
#     venue = resp.json()['response']
#     res = {}
#     res['venue'] = venue['venue']
#     SocialDataRepository.addFQVenue(res)


# parquetFile = spark.read.parquet("FQ_VENUE.parquet")
# print(parquetFile.where(parquetFile.venueid == "4c034d0cf56c2d7fa6c71c66").count())

    # parquetFile.show()

# ex ={}
# ex['keyword'] = "abc"
# print(SocialDataRepository.addPlaceOrQuery(ex))

# data = {
#     "hereNow": {
#       "count": 0,
#       "items": []
#     },
#     "venueId" : "4c034d0cf56c2d7fa6c71c66"
#   }

# r = requests.post('http://203.151.85.73:5001/foursquare/addCheckin', json=data)
# print(r.status_code)


# parquetFile = spark.read.parquet("PLACE.parquet")
# parquetFile.show()

# parquetFile = spark.read.parquet("QUERY.parquet")
# parquetFile.show()

# parquetFile = spark.read.parquet("FQ_VENUE.parquet")
# parquetFile.show()

# print(parquetFile.toJSON)
# parquetFile.show()


# print(SocialDataRepository.getAllFQVenue())

# with open('fqTip.json') as json_data:
#     tipJSON = json.load(json_data)
#     SocialDataRepository.addFQTips(tipJSON)


with open('fqCheckin.json') as json_data:
    tipJSON = json.load(json_data)
    SocialDataRepository.addFQCheckin(tipJSON)

parquetFile = spark.read.parquet("SOCIALDATA.parquet")
parquetFile.where(parquetFile.source== "FQ_CHECKIN").show()