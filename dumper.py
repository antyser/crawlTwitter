__author__ = 'junliu'
import time, sys
from time import gmtime, strftime
import kafkaUtil
import json

def consume(kafka_host, topic):
    consumer = kafkaUtil.create_consumer(kafka_host, "172.31.24.55:2181", topic, "monitor_test")
    current_timestamp = int(time.time())
    counter = 0
    filename = topic + "." + str(current_timestamp)
    with open(filename, 'w') as out:
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
    print 'usage: python dumper.py <kafka-host:port> <topic>'
    consume(sys.argv[1], sys.argv[2])
