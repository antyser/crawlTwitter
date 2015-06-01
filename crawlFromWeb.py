import urllib2
from lxml import html
from lxml.etree import tostring
import httplib
import time
import logging
import json
import sys, os
import yaml
import kafkaUtil
CONTINUE_URL = "https://twitter.com/i/profiles/show/{0}/timeline?contextual_tweet_id={1}&include_available_features=1&include_entities=1&max_position={1}"

def load_config():
    with open(os.path.join(sys.path[0], 'config.yml'), 'r') as fl:
        cnf = yaml.load(fl)
        return cnf


def consume(kafka_host, cfg):
    consumer = kafkaUtil.create_consumer(kafka_host, cfg['zookeeper'], cfg['kafka']['seeds'], 'fetcher')
    producer = kafkaUtil.create_producer(kafka_host, cfg['kafka']['pages'])
    print "start consuming"
    while True:
        msg = consumer.consume()
        if msg is None:
            continue
        print "input:", msg.offset, msg.value
        seed = json.loads(msg.value)
        process(seed, producer, cfg)
        time.sleep(2)

def produce(data, producer):
    try:
        producer.produce([data])
    except Exception as err:
        logging.warning(err)

def get_html(url):
    logging.debug("requesting " + url)
    while True:
        try:
            req = urllib2.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            text = urllib2.urlopen(req).read()
            return text
        except httplib.BadStatusLine as e:
            logging.error(e + " stand down for 10 mins...")
            time.sleep(600)
        except Exception as e:
            print logging.error(e)
            return None


def process(seed, producer, cfg):
    html_content = get_html(seed['url'])
    if html_content == None:
        return
    data = dict()
    data['data'] = html_content
    data['seed'] = seed['url']
    data['download_timestamp'] = time.time()
    produce(json.dumps(data), producer)
    # tree = html.fromstring(html_content)
    # result = tree.xpath("//div[@class='StreamItem js-stream-item']")
    # for tweet in result:
    #     data = dict()
    #     data['twitter_id'] = tweet.attrib['data-item-id']
    #     data['data'] = tostring(tweet)
    #     data['seed'] = seed['url']
    #     data['download_timestamp'] = time.time()
    #     produce(json.dumps(data), producer)
    #grid_item_line = tree.xpath("//div[@class='GridTimeline-items']")
    #max_tweet_id = grid_item_line[0].attrib['data-min-position']


    #for i in range(0, 0):
    #    json_text = get_html(CONTINUE_URL.format(user_name, max_tweet_id))
    #    addtion_data = json.loads(json_text)
    #    tree = html.fromstring(addtion_data['items_html'])
    #    result = tree.xpath("//div[@class='StreamItem js-stream-item']")
    #    for tweet in result:
    #        data = dict()
    #        data['id'] = tweet.attrib['data-item-id']
    #        data['raw'] = tostring(tweet)
    #       persist_data(json.dumps(data), producer)
    #    if not addtion_data['has_more_items']:
    #        logging.info("no more items")
    #        break
    #    max_tweet_id = addtion_data['min_position']
    #    time.sleep(5)

if __name__ == '__main__':
    print 'usage: python crawlFromWeb.py <kafka-host:port>'
    logging.basicConfig(file='fetch.log', level=logging.INFO)
    kafka_host = "172.31.10.154:9092"
    if len(sys.argv) == 2:
        kafka_host = sys.argv[1]
    cfg = load_config()
    consume(kafka_host, cfg)
