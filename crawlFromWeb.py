import urllib2
from lxml import html
from lxml.etree import tostring
import httplib
import time
import logging
import json
import sys
from kafka import SimpleProducer, KafkaClient, SimpleConsumer
from kafka.common import MessageSizeTooLargeError

CONTINUE_URL = "https://twitter.com/i/profiles/show/{0}/timeline?contextual_tweet_id={1}&include_available_features=1&include_entities=1&max_position={1}"

def fetchFrom(kafka_host):
    kafka = KafkaClient(kafka_host)
    consumer = SimpleConsumer(kafka, 'fetcher', 'crawl.twitter.seeds.0520')
    producer = SimpleProducer(kafka)

    for msg in consumer:
        print msg.message.value
        seed = json.loads(msg.message.value)
        process_twitter_account(seed, producer)
        time.sleep(2)

    kafka.close()

def persist_data(data, producer):
    try:
        producer.send_messages("crawl.twitter.pages.0520", data)
    except MessageSizeTooLargeError as err:
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


def process_twitter_account(seed, producer):
    html_content = get_html(seed['url'])
    if html_content == None:
        return
    tree = html.fromstring(html_content)
    result = tree.xpath("//div[@class='StreamItem js-stream-item']")
    for tweet in result:
        data = dict()
        data['twitter_id'] = tweet.attrib['data-item-id']
        data['data'] = tostring(tweet)
        data['seed'] = seed['url']
        data['download_timestamp'] = time.time()
        persist_data(json.dumps(data), producer)
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
    fetchFrom(kafka_host)
