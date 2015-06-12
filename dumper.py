__author__ = 'junliu'
import time, sys
from time import gmtime, strftime
import kafkaUtil
import json

def consume(kafka_host, topic, filename):
    consumer = kafkaUtil.create_consumer(kafka_host, "172.31.24.55:2181", topic, "monitor")
    current_timestamp = int(time.time())
    counter = 0
    with open(filename, 'a') as out:
        while True:
            msg = consumer.consume()
            if msg is None:
                continue
            jsonobj = json.loads(msg)
            if jsonobj['ts_fetch'] > current_timestamp:
                break
            out.write(msg.value)
            out.write('\n')
            counter += 1
        out.write("till " + str(current_timestamp), "consume " + str(counter))


if __name__ == '__main__':
    print 'usage: python dumper.py <kafka-host:port> <topic> <filename>'
    consume(sys.argv[1], sys.argv[2], sys.argv[3])