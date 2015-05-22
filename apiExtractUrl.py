__author__ = 'junliu'
import json
import logging
from urlparse import urlparse
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError

HEADERS = {'User-agent': 'Mozilla/5.0'}
QUERY_URL = "http://api.longurl.org/v2/expand?format=json&url="
PAGES_TOPIC = 'api.twitter.pages.0522'
LINKS_TOPIC = "api.twitter.links.0522"



def produce(data, producer, topic):
    try:
        producer.send_messages(topic, data)
    except MessageSizeTooLargeError as err:
        logging.warning(err)

def process(data, producer):
    if not 'entities' in data:
        return
    urls = data['entities']['urls']
    for url in urls:
        output = dict()
        url_destination = url['expanded_url']
        output['domain'] = urlparse(url_destination).netloc
        output['url'] = url_destination
        output['score'] = 1
        if 'download_timestamp' in data:
            output['download_timestamp'] = data['download_timestamp']
        print output
        produce(json.dumps(output), producer, LINKS_TOPIC)

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
    logging.basicConfig(file='api_extract.log', level=logging.INFO)
    kafka_host = "172.31.10.154:9092"
    consume(kafka_host)
