__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient
import json
import sys


if __name__ == '__main__':
    kafka_host = '172.31.10.154:9092'
    if len(sys.argv) == 2:
        kafka_host = sys.argv[1]
    kafka = KafkaClient()
    producer = SimpleProducer(kafka)
    print 'connected to kafka, start populating seeds'

    INITIAL_URL = "https://twitter.com/{0}"
    with open("twitter_account.txt", "r") as indoc:
        for line in indoc:
            seed = dict()
            seed['url'] = INITIAL_URL.format(line.strip())
            producer.send_messages("crawl.twitter.seeds.0520", json.dumps(seed))
    kafka.close()