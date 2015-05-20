import urllib2
from lxml import html
from lxml.etree import tostring
import httplib
import time
import logging
import json

INITIAL_URL = "https://twitter.com/{0}"
CONTINUE_URL = "https://twitter.com/i/profiles/show/{0}/timeline?contextual_tweet_id={1}&include_available_features=1&include_entities=1&max_position={1}"
logging.basicConfig(filename='/tmp/twitter.log',level=logging.DEBUG)

def persist_data(data):
    with open('data1.txt', 'a+') as file:
        file.write(data + "\n")


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
            time.sleep(600)


def process_twitter_account(user_name):
    text = get_html(INITIAL_URL.format(user_name))
    tree = html.fromstring(text)
    grid_item_line = tree.xpath("//div[@class='GridTimeline-items']")
    max_tweet_id = grid_item_line[0].attrib['data-min-position']
    result = tree.xpath("//div[@class='StreamItem js-stream-item']")
    for tweet in result:
        data = dict()
        data['id'] = tweet.attrib['data-item-id']
        data['raw'] = tostring(tweet)
        persist_data(json.dumps(data))

    for i in range(0, 0):
        json_text = get_html(CONTINUE_URL.format(user_name, max_tweet_id))
        addtion_data = json.loads(json_text)
        tree = html.fromstring(addtion_data['items_html'])
        result = tree.xpath("//div[@class='StreamItem js-stream-item']")
        for tweet in result:
            data = dict()
            data['id'] = tweet.attrib['data-item-id']
            data['raw'] = tostring(tweet)
            persist_data(json.dumps(data))
        if not addtion_data['has_more_items']:
            logging.info("no more items")
            break
        max_tweet_id = addtion_data['min_position']
        time.sleep(5)

# process_twitter_account('007')
with open("twitter_account.txt") as file:
    for user_name in file:
        process_twitter_account(user_name.strip())
