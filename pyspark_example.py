from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "meetup-sparkstreaming") \
    .option("startingOffsets", "latest") \
    .load()
lines = lines.rdd.map(lambda x: x).toDF().writeStream.outputMode("complete").format("console").start()
# lines.show()

lines.awaitTermination()