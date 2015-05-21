__author__ = 'junliu'
import json
import logging
import sys
import urllib
from lxml import html
from urlparse import urlparse
import time, requests
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError

HEADERS = {'User-agent': 'Mozilla/5.0'}
QUERY_URL = "http://api.longurl.org/v2/expand?format=json&url="
PAGES_TOPIC = 'crawl.twitter.pages.0520'
LINKS_TOPIC = "crawl.twitter.links.0520"



def produce(data, producer, topic):
    try:
        producer.send_messages(topic, data)
    except MessageSizeTooLargeError as err:
        logging.warning(err)

    #data {"seed": , "data": , "try"}
def process(data, producer):
    if 'try' in data and data['try'] >= 3:
        logging.info("too many retries, dump the url: " + data['data'])
        return
    url = data['data']
    tree = html.fromstring(url)
    result = tree.xpath(
        "//div[@class='StreamItem js-stream-item']/div/div[2]/p//a[@class='twitter-timeline-link']/@href")
    for url in result:
        output = dict()
        response = requests.get(QUERY_URL+url, headers=HEADERS)
        if response.status_code != requests.codes.ok:
            logging.warning(str(response.status_code)+'\t'+ response.reason)
            if not 'try' in data:
                data['try'] = 1
            else:
                data['try'] += 1
            print data
            produce(json.dumps(data), producer, PAGES_TOPIC)
            return
        json_obj = json.loads(response.text)
        url_destination = json_obj['long-url']
        output['domain'] = urlparse(url_destination).netloc
        output['url'] = url_destination
        output['score'] = 1
        if 'download_timestamp' in data:
            output['download_timestamp'] = data['download_timestamp']
        print output
        produce(json.dumps(output), producer, LINKS_TOPIC)
        time.sleep(6)


def consume(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', PAGES_TOPIC)
    producer = SimpleProducer(kafka)
    consumer.max_buffer_size=20*1024*1024
    for msg in consumer:
        page = json.loads(msg.message.value)
        process(page, producer)
    kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='extract.log', level=logging.INFO)
    kafka_host = "172.31.10.154:9092"
    consume(kafka_host)
