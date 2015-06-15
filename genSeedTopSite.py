from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError
import json, sys, requests, logging, time


HEADERS = {'User-agent': 'Mozilla/5.0'}

def fetchUrl(url, producer):
    r = requests.get(url, headers=HEADERS)
    logging.info('fetch: '+url)

    if r.status_code == requests.codes.ok:
        page = {'seed': url, 'data': r.text, 'ts_task': int(time.time()), 'label': 'toppage'}
        try:
            producer.send_messages("seeds", json.dumps(page))
        except MessageSizeTooLargeError as err:
            logging.warning(err)
    else:
        logging.warning(str(r.status_code)+'\t'+ r.reason)


def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', 'topsite.links')
    producer = SimpleProducer(kafka)

    for msg in consumer:
        topsites = json.loads(msg.message.value)
        for url in topsites['links']:
            seed = {}
            seed['url'] = url
            seed['ts_task'] = int(time.time())
            seed['label'] = 'toppage'
            try:
                producer.send_messages("seeds", json.dumps(seed))
            except MessageSizeTooLargeError as err:
                logging.warning(err)

    kafka.close()


if __name__ == '__main__':
    print 'USAGE:  python genSeedTopSite.py'
    logging.basicConfig(file='fetch.log', level=logging.INFO)

    kafka_host = '172.31.10.154:9092'

    fetchFrom(kafka_host)
