def writeParquet(parquetPath, data, sparkContext, sparkSession):
    if len(data) > 0:
        rowRDD = sparkContext.parallelize(data)
        rowDF = sparkSession.createDataFrame(rowRDD)
        rowDF.write.mode("append").parquet(parquetPath)
    else:
        print("no row written")