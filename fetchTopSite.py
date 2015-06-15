from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError
import json, sys, requests, logging, time

HEADERS = {'User-agent': 'Mozilla/5.0'}


def fetchUrl(url, producer):
    r = requests.get(url, headers=HEADERS)
    logging.info('fetch: ' + url)

    if r.status_code == requests.codes.ok:
        page = {}
        page['url'] = url
        page['content'] = r.text
        page['ts_fetch'] = int(time.time())
        try:
            producer.send_messages("toppage.pages", json.dumps(page))
        except MessageSizeTooLargeError as err:
            logging.warning(err)
    else:
        logging.warning(str(r.status_code) + '\t' + r.reason)


def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'tmp', 'topsite.pages')
    producer = SimpleProducer(kafka)

    for msg in consumer:
        obj = json.loads(msg.message.value)
        for url in obj['links']:
            fetchUrl(url, producer)

    kafka.close()


if __name__ == '__main__':
    logging.basicConfig(file='fetch.log', level=logging.INFO)

    kafka_host = '172.31.10.154:9092'

    fetchFrom(kafka_host)
