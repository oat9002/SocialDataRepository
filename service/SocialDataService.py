def writeParquet(parquetPath, data, sparkContext, sparkSession, schema):
    if len(data) > 0:
        rowRDD = sparkContext.parallelize(data)
        rowDF = sparkSession.createDataFrame(rowRDD, schema)
        rowDF.write.mode("append").parquet(parquetPath)
    else:
        print("no row written")
