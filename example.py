from pyspark import SparkContext
import json


sc = SparkContext("local", "some")
lines = sc.textFile("x.json", )
lines = lines.map(lambda x: x).take(10)
print(lines)
# sc = sc.getOrCreate()
# with open("x.json", 'r') as f:
#     l = f.read()
#     json.loads(l)
#     print(l)

