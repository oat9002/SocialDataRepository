# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pandas.io.json import json_normalize
from geopy.distance import great_circle
import uuid
import os.path as path
import datetime
import dateutil.parser as date
import pydoop.hdfs

hdfs = pydoop.hdfs.hdfs()

def writeParquet(parquetFile,rowArr, sc, spark):
    if len(rowArr) > 0:
        rowRDD = sc.parallelize(rowArr)
        rowDF = spark.createDataFrame(rowRDD)
        rowDF.write.mode("append").parquet(parquetFile)
    else:
        print("no row written")

#FB_PAGE####################################
def savePage(page,queryId, sc, spark):
    pageParquet = "hdfs://stack-02:9000/SocialDataRepository/FB_PAGE.parquet"
    if hdfs.exists(pageParquet):     
        pageBaseDF = spark.read.parquet(pageParquet)
        existPage = pageBaseDF.where(pageBaseDF.pageid == page['id'])
        if existPage.count() == 0:
            writeParquet(pageParquet,[selectPageCol(page,queryId)], sc, spark)
    else:
        writeParquet(pageParquet,[selectPageCol(page,queryId)], sc, spark)       
def selectPageCol(page,queryId):
    newPage = {}
    newPage['pageid'] = page['id']
    newPage['query_id'] = queryId
    return newPage

#FB_POST####################################
def savePost(posts,pageId, sc, spark):
    postParquet = "hdfs://stack-02:9000/SocialDataRepository/FB_POST.parquet"
    allPost = []
    if hdfs.exists(postParquet):     
        postBaseDF = spark.read.parquet(postParquet)
        for post in posts['data']:
            existPost = postBaseDF.where(postBaseDF.postid == post['id'])
            if existPost.count() == 0:
                allPost.append(selectPostCol(post,pageId))
        writeParquet(postParquet,allPost, sc, spark)     
    else:
        for post in posts['data']:
            allPost.append(selectPostCol(post,pageId))
        writeParquet(postParquet,allPost, sc, spark)     

def selectPostCol(post,pageId):
    newPost = {}
    newPost['postid'] = post['id']
    newPost['created_at'] = post['created_time']
    if 'message' in post:
        newPost['message'] = post['message']
    newPost['pageid'] = pageId  
    return newPost

#FB_COMMENT####################################
def saveComment(comments,postId, sc, spark):
    commentParquet = "hdfs://stack-02:9000/SocialDataRepository/FB_COMMENT.parquet"
    allComment = []
    if hdfs.exists(commentParquet):     
        commentBaseDF = spark.read.parquet(commentParquet)
        for comment in comments['data']:
            existComment = commentBaseDF.where(commentBaseDF.commentid == comment['id'])
            if existComment.count() == 0:
                allComment.append(selectCommentCol(comment,postId))
        writeParquet(commentParquet,allComment, sc, spark)     
    else:
        for comment in comments['data']:
            allComment.append(selectCommentCol(comment,postId))
        writeParquet(commentParquet,allComment, sc, spark)     

def selectCommentCol(comment,postId):
    newComment = {}
    newComment['commentid'] = comment['id']
    newComment['created_at'] = comment['created_time']
    if 'message' in comment:
        newComment['message'] = comment['message']
    newComment['postid'] = postId  
    newComment['userid'] = comment['from']['id']
    return newComment

#FB_USER####################################
def saveUser(user, sc, spark):
    userParquet = "hdfs://stack-02:9000/SocialDataRepository/FB_USER.parquet"
    if hdfs.exists(userParquet):     
        userBaseDF = spark.read.parquet(userParquet)
        existUser = userBaseDF.where(userBaseDF.userid == user['id'])
        if existUser.count() == 0:
            writeParquet(userParquet,[selectUserCol(user)], sc, spark)
    else:
        writeParquet(userParquet,[selectUserCol(user)], sc, spark)       
def selectUserCol(user):
    newUser = {}
    newUser['userid'] = user['id']
    newUser['name'] = user['name']
    return newUser
