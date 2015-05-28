__author__ = 'junliu'
import datetime, sys
from time import gmtime, strftime
import kafkaUtil

def consume(kafka_host, topic, filename):
    consumer = kafkaUtil.create_consumer(kafka_host, "172.31.24.55:2181", topic, strftime("%Y%m%d%H%M%S", gmtime()))
    while True:
        with open(filename, 'a') as out:
            msg = consumer.consume()
            if msg is None:
                continue
            out.write(msg)
            out.write('\n')


if __name__ == '__main__':
    print 'usage: python extractUrl.py <kafka-host:port> <topic> <filename>'
    consume(sys.argv[1], sys.argv[2], sys.argv[3])