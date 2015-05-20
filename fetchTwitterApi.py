__author__ = 'junliu'
import tweepy
import logging
import time
import json
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('/var/tmp/myapp.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)
def persist_data(data):
    filename = "data.txt"
    with open(filename, "a+") as myfile:
        for line in data:
            myfile.write(line)


# Consumer keys and access tokens, used for OAuth
consumer_key = 'vzSxDZg30XEpwvKoVpoQwrZ9Y'
consumer_secret = 'GJU9sIpmfXOOpS8XfEINE7v4qNzpUdyjaXKAyx2IUx3kJr5Y5k'
access_token = '3220668044-sPijKWEAHCjPPFriNF5wqDuojrCrP7qZ4OcDyD1'
access_token_secret = '4lBtGD08biSUrbMHlL6IIJ4yGyZ2zxZldjOGidqII6Ziz'

# OAuth process, using the keys and tokens
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Creation of the actual interface, using authentication
api = tweepy.API(auth)
prev_id = "600798141483339776"
current_status = []
while True:
    stuff = api.home_timeline(prev_id, count=800)
    for status in stuff:
        current_status.append(json.dumps(status._json))
    if len(current_status) > 0:
        prev_id = json.loads(current_status[0])["id"]
    print "receive " + str(len(current_status))
    print prev_id
    print
    #logger.info("receive {}".format(len(current_status)))
    persist_data(current_status)
    current_status = []
    time.sleep(60)
