# In order to work as is on Data Fabric, this file should be inside a zip archive.
from pyspark.sql import SparkSession

# Spark Session and DataFrame creation
sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()
data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
df = sparkSession.createDataFrame(data)


# Write into HDFS
df.write.csv("hdfs://cluster/user/hdfs/test/example.csv")

# Read from HDFS
df_load = sparkSession.read.csv('hdfs://cluster/user/hdfs/test/example.csv')
df_load.show()
