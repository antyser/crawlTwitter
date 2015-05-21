__author__ = 'junliu'
import json
import logging
import sys
import urllib2
from lxml import html
from urlparse import urlparse
import time, requests
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError

HEADERS = {'User-agent': 'Mozilla/5.0'}
QUERY_URL = "http://api.longurl.org/v2/expand?format=json&url="

def persist_data(data, producer):
    try:
        producer.send_messages("crawl.twitter.links.0520", data)
    except MessageSizeTooLargeError as err:
        logging.warning(err)


def parse_html(url, producer):
    tree = html.fromstring(url)
    result = tree.xpath(
        "//div[@class='StreamItem js-stream-item']/div/div[2]/p//a[@class='twitter-timeline-link']/@href")
    for url in result:
        data = dict()
        response = requests.get(QUERY_URL+url, headers=HEADERS)
        if response.status_code != requests.codes.ok:
            logging.warning(str(response.status_code)+'\t'+ response.reason)
            return
        json_obj = json.loads(response.text)
        url_destination = json_obj['long-url']
        data['domain'] = urlparse(url_destination).netloc
        data['url'] = url_destination
        data['score'] = 1
        print data
        persist_data(json.dumps(data), producer)
        time.sleep(6)


def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', 'crawl.twitter.pages.0520')
    producer = SimpleProducer(kafka)

    for msg in consumer:
        page = json.loads(msg.message.value)
        url = page['data']
        parse_html(url, producer)
    kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = "172.31.10.154:9092"
    fetchFrom(kafka_host)
