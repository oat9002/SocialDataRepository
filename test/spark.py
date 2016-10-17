from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .getOrCreate()

# # spark is an existing SparkSession
# df = spark.read.json("./people.json")
#
# # Displays the content of the DataFrame to stdout
# df.show()

# # Print the schema in a tree format
# df.printSchema()
#
# # Select only the "name" column
# df.select("name").show()

sc = spark.sparkContext

# Load a text file and convert each line to a Row.
lines = sc.textFile("./people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)

# DataFrames can be saved as Parquet files, maintaining the schema information.
schemaPeople.write.mode("append").parquet("people.parquet")

# Read in the Parquet file created above. Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
parquetFile = spark.read.parquet("people.parquet")

parquetFile.show()
# Parquet files can also be used to create a temporary view and then used in SQL statements.
parquetFile.createOrReplaceTempView("parquetFile")
teenagers = spark.sql("SELECT * FROM parquetFile")
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name + " Age: " + str(p.age))
for teenName in teenNames.collect():
  print(teenName)

# schemaPeople.createOrReplaceTempView("people")

# # SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
#
# # The results of SQL queries are RDDs and support all the normal RDD operations.
# teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name)
# for teenName in teenNames.collect():
#   print(teenName)

# logFile = "./text.txt"  # Should be some file on your system
# sc = SparkContext("local", "Simple App")
# logData = sc.textFile(logFile).cache()
#
# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()
# As = logData.filter(lambda s: 'a' in s)
# Bs = logData.filter(lambda s: 'b' in s)
#
# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
# print("numAs: ", As, " numBs: ", Bs)
