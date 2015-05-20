__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient

kafka = KafkaClient("172.31.10.154:9092")
producer = SimpleProducer(kafka)
print 'connected to kafka, start populating seeds'


with open("twitter_account.txt", "r") as indoc:
    for line in indoc:
        producer.send_messages("crawl.twitter.seeds.0520", line.strip())
kafka.close()