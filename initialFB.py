# -*- coding: utf-8 -*-
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext
from repository import FacebookRepository
from repository import SocialDataRepository
from pandas.io.json import json_normalize
import dateutil.parser as date
import json
import os.path as path
import datetime

spark = SparkSession\
    .builder\
    .appName("Initial FacebookRepository")\
    .getOrCreate()

sc = spark.sparkContext

with open('fbPage.json') as json_data:
    pageJSON = json.load(json_data)
    FacebookRepository.savePage(pageJSON, 12453)

    parquetFile = spark.read.parquet("FB_PAGE.parquet")
    parquetFile.show()


with open('fbPost.json') as json_data:
    postJSON = json.load(json_data)
    FacebookRepository.savePost(postJSON, 12453)

    parquetFile = spark.read.parquet("FB_POST.parquet")
    parquetFile.show()

with open('fbComment.json') as json_data:
    commentJSON = json.load(json_data)
    FacebookRepository.saveComment(commentJSON, 12453)

    parquetFile = spark.read.parquet("FB_COMMENT.parquet")
    parquetFile.show()

    FacebookRepository.saveUser(commentJSON['data'][0]['from'])

    parquetFile = spark.read.parquet("FB_USER.parquet")
    parquetFile.show()
 