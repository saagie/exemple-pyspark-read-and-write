from pyspark import SparkContext
from pyspark.sql import SparkSession

# We have to set the hive metastore uri.
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://nn1:9083")

# Spark Session and DataFrame creation
sparkSession = (SparkSession
                .builder
                .appName('example-pyspark-read-and-write-from-hive')
                .enableHiveSupport()
                .getOrCreate())

data = [('First', 1), ('Second', 2), ('Third', 3), ('Fourth', 4), ('Fifth', 5)]
df = sparkSession.createDataFrame(data)

# Write into Hive
df.write.saveAsTable('example')

# Read from Hive
df_load = sparkSession.sql('SELECT * FROM example')
df_load.show()
