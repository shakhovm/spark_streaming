from kafka import KafkaConsumer
import json
import sys
import time
import ast


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
        msg.value['cities'] = list(set(msg.value['cities']))
        lst.append(msg.value)
        if (time.time() - start) >= time_duration:
            break

    with open(cfg['file_out'], 'w', encoding='utf-8') as f:
        json.dump(lst, f)