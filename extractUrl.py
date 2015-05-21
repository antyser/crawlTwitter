__author__ = 'junliu'
import json
import logging
import sys
from kafka import SimpleProducer, KafkaClient, SimpleConsumer


def parse_html(url):
    pass


def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', 'crawl.twitter.pages.0520')
    producer = SimpleProducer(kafka)

    for msg in consumer:
        print msg.message.value
        page = json.loads(msg.message.value)
        url = page['url']
        parse_html(url)

    kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='fetch.log', level=logging.INFO)

    kafka_host = "172.31.10.154:9092"

    fetchFrom(kafka_host)