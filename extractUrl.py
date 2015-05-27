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
cfg = load_config()
PAGES_TOPIC = cfg['kafka']['pages']
LINKS_TOPIC = cfg['kafka']['links']

def load_config():
    with open('config.yml', 'r') as fl:
        cnf = yaml.load(fl)
        return cnf

def produce(data, producer, topic):
    try:
        producer.send_messages(topic, data)
    except MessageSizeTooLargeError as err:
        logging.warning(err)

    #data {"seed": , "data": , "try"}
def process(data, producer):
    try:
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
            time.sleep(2)
    except Exception as err:
        logging.error(err)


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
    print 'usage: python extractUrl.py <kafka-host:port>'
    logging.basicConfig(file='extract.log', level=logging.INFO)
    kafka_host = "172.31.10.154:9092"
    if len(sys.argv) == 2:
        kafka_host = sys.argv[1]
    consume(kafka_host)