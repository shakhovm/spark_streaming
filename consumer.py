from kafka import KafkaConsumer
import json
import sys
import time
import ast
from save_to_bucket import save_json_to_cloud


if __name__ == "__main__":
    with open(sys.argv[1], 'r') as f:
        cfg = json.load(f)
    consumer = KafkaConsumer(
        cfg['topic'],
        bootstrap_servers=[cfg['host']],
        value_deserializer=lambda x: ast.literal_eval(x.decode('UTF-8'))
    )
    time_duration = cfg["time_duration"]

    lst = []

    start = time.time()
    for msg in consumer:
        print(msg.value)
        lst.append(msg.value)
        if (time.time() - start) >= time_duration:
            break

    save_json_to_cloud(cfg['file_out'], lst, cfg["key_json"], cfg["bucket_name"])