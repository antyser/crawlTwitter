__author__ = 'junliu'
import json
import logging
import sys, os
from lxml import html
from urlparse import urlparse
import time, requests
import yaml
import kafkaUtil

HEADERS = {'User-agent': 'Mozilla/5.0'}
QUERY_URL = "http://api.longurl.org/v2/expand?format=json&url="


def load_config():
    with open(os.path.join(sys.path[0], 'config.yml'), 'r') as fl:
        cnf = yaml.load(fl)
        return cnf


def produce(data, producer):
    print 'output:', data
    try:
        producer.produce([data])
    except Exception as err:
        logging.warning(err)

        # data {"seed": , "data": , "try"}


def expend_url(url):
    print 'url:', url
    response = requests.get(QUERY_URL + url, headers=HEADERS)
    if response.status_code != requests.codes.ok:
        logging.warning(str(response.status_code) + '\t' + response.reason)
        if response.status_code == 404:
            return None
        # TODO:should put in failure queue
        # produce(json.dumps(data), producers['page'])
        return None
    json_obj = json.loads(response.text)
    return json_obj['long-url']


def process(data, producers, cfg):
    try:
        content = data['data']
        tree = html.fromstring(content)
        result = tree.xpath("//*[@class='content']/p//a[@class='twitter-timeline-link']")
        for a_tag in result:
            output = {}
            tiny_url = a_tag.attrib['href']
            url_destination = expend_url(tiny_url)
            print 'expended url:', url_destination
            output['domain'] = urlparse(url_destination).netloc
            output['url'] = url_destination
            output['score'] = 1
            output['download_timestamp'] = data['download_timestamp']
            produce(json.dumps(output), producers['links'])
            time.sleep(1)
    except Exception as err:
        logging.error(err)


def consume(kafka_host, cfg):
    consumer = kafkaUtil.create_consumer(kafka_host, cfg['zookeeper'], cfg['kafka']['pages'], cfg['kafka']['consume_group'])
    link_producer = kafkaUtil.create_producer(kafka_host, cfg['kafka']['links'])
    page_producer = kafkaUtil.create_producer(kafka_host, cfg['kafka']['pages'])
    producers = {'links': link_producer, 'pages': page_producer}
    while True:
        msg = consumer.consume()
        if msg is None:
            continue
        page = json.loads(msg.value)
        process(page, producers, cfg)



if __name__ == '__main__':
    print 'usage: python extractUrl.py <kafka-host:port>'
    logging.basicConfig(file='extract.log', level=logging.INFO)
    kafka_host = "172.31.10.154:9092"
    if len(sys.argv) == 2:
        kafka_host = sys.argv[1]
    cfg = load_config()
    while True:
        try:
            consume(kafka_host, cfg)
        except Exception as err:
            logging.error(err)
