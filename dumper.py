__author__ = 'junliu'
import datetime, sys
from time import gmtime, strftime

def consume(kafka_host, queue_name, filename):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, strftime("%Y%m%d%H%M%S", gmtime()), queue_name)
    consumer.max_buffer_size=20*1024*1024
    for msg in consumer:
        with open(filename, 'a') as out:
            json.dump(msg.message.value, out)
            out.write('\n')
    kafka.close()


if __name__ == '__main__':
    print 'usage: python extractUrl.py <kafka-host:port>, <topic> <filename>'
    logging.basicConfig(file='extract.log', level=logging.INFO)
    consume(sys.argv[1], sys.argv[2], sys.argv[3])