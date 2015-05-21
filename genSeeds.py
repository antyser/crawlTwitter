__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient
import json

kafka = KafkaClient("172.31.10.154:9092")
producer = SimpleProducer(kafka)
print 'connected to kafka, start populating seeds'

INITIAL_URL = "https://twitter.com/{0}"
with open("twitter_account.txt", "r") as indoc:
    for line in indoc:
        seed = dict()
        seed['url'] = INITIAL_URL.format(line.strip())
        producer.send_messages("crawl.twitter.seeds.0520", json.dumps(seed))
kafka.close()