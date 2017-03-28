def writeParquet(parquetPath, data, sparkContext, sparkSession):
    if len(data) > 0:
        rowRDD = sparkContext.parallelize(data)
        rowDF = sparkSession.createDataFrame(rowRDD)
        rowDF.write.mode("append").parquet(parquetPath)
    else:
        print("no row written")

def writeParquetWithSchema(parquetPath, data, schema, sparkContext, sparkSession):
    if len(data) > 0:
        rowRDD = sparkContext.parallelize(data)
        rowDF = sparkSession.createDataFrame(rowRDD, schema)
        rowDF.write.mode("append").parquet(parquetPath)
    else:
        print("no row written")
