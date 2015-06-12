__author__ = 'junliu'
import time, sys
from kafka import KafkaConsumer
import kafkaUtil
import json


def consume(kafka_host, topic):
    consumer = KafkaConsumer(topic,
                             group_id='monitor_test',
                             bootstrap_servers=[kafka_host],
                             auto_commit_enable=True,
                             auto_commit_interval_ms=1000,
                             auto_offset_reset='smallest',
                             fetch_message_max_bytes=20 * 1024 * 1024
                             )
    current_timestamp = int(time.time())
    counter = 0
    filename = topic + "." + str(current_timestamp)
    with open(filename, 'w') as out:
        for msg in consumer:
            if msg is None:
                continue
            jsonobj = json.loads(msg.value)
            if jsonobj['ts_fetch'] > current_timestamp:
                break
            out.write(msg.value)
            out.write('\n')
            counter += 1
        out.write("till " + str(current_timestamp), "consume " + str(counter))

if __name__ == '__main__':
    print 'usage: python dumper.py <kafka-host:port> <topic>'
    consume(sys.argv[1], sys.argv[2])
