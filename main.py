import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys
from kafka import KafkaProducer
from pyspark import SparkContext
import json
from datetime import datetime
from constants import topic_name1, topic_name2, topic_name3
from constants import prg_topics, states


def time_to_format(t):
    return datetime.strftime(datetime.fromtimestamp(t / 1000.0), "%Y.%m.%d-%H:%M:%S")


def filter_topics(x):
    for topic in x["group"]["group_topics"]:
        if topic["topic_name"] in prg_topics:
            return True
    return False


def handler(message, topic, producer):
    records = message.take(1000000000000000000000)  # collect doesn't work :(
    for record in records:
        producer.send(topic, str(record))


def to_appropriate_format(x):
    current_time = datetime.now()
    return {
        "cities": [x["group"]["group_city"]],
        "day": current_time.day,
        "month": current_time.month,
        "minute": current_time.minute,
        "year": current_time.year
    }


def reduce_data(x, y):
    x["cities"] += y["cities"]
    return x


if __name__ == "__main__":
    print(pyspark.__version__)
    producer = KafkaProducer(bootstrap_servers=sys.argv[1],
                             value_serializer=lambda x: x.encode('UTF-8'))

    sc = SparkContext("local[*]", "MeetsUp")
    sc.setLogLevel("ERROR")

    ssc = StreamingContext(sc, 5)
    kvs = KafkaUtils.createStream(ssc, sys.argv[2], "1", {"meetsup-input": 3})
    us_ss = kvs.map(lambda x: json.loads(x[1])) \
        .filter(lambda x: x['group']['group_country'] == 'us')

    us_meetsup = us_ss.map(lambda x: {
        "mtime": time_to_format(x["mtime"]),
        "event": {
            "event_name": x["event"]["event_name"],
            "event_id": x["event"]["event_id"],
            "time": time_to_format(x["event"]["time"])

        },
        "group_city": x["group"]["group_city"],
        "group_id": x["group"]["group_id"],
        "group_name": x["group"]["group_name"],
        "group_state": states[x["group"]["group_state"]],
    }) \
        .foreachRDD(lambda x: handler(x, topic_name1, producer))

    us_cities = us_ss.window(60, 60) \
        .map(to_appropriate_format) \
        .reduce(reduce_data).foreachRDD(lambda x: handler(x, topic_name2, producer))

    prog_meetups = us_ss.filter(lambda x: filter_topics).map(lambda x: {
        "mtime": time_to_format(x["mtime"]),
        "event": {
            "event_name": x["event"]["event_name"],
            "event_id": x["event"]["event_id"],
            "time": time_to_format(x["event"]["time"])

        },
        "group_topics": list(map(lambda y: y["topic_name"], x["group"]["group_topics"])),
        "group_city": x["group"]["group_city"],
        "group_id": x["group"]["group_id"],
        "group_name": x["group"]["group_name"],
        "group_state": states[x["group"]["group_state"]],
    }) \
        .foreachRDD(lambda x: handler(x, topic_name3, producer))

    ssc.start()
    ssc.awaitTermination()
