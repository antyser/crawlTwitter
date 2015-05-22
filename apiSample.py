__author__ = 'junliu'
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

#consumer_key="0QYtYUshr2SiccZL8zbMdq8uW"
#consumer_secret="xi9w0KZqQ4MKJQAzPT7CUeG05S8yh8KqFxfwgC8BaHD7ZKyDjL"

#access_token="3220668044-4Kb8T9TxdwgvpS1lG7CijYhexBrsyHuCNVlgaWs"
#access_token_secret="wWBY3Er3TdOByR3M0Y7Kc9nJb8wKhJFRbNfnwJ6Dqfm3E"

consumer_key="vzSxDZg30XEpwvKoVpoQwrZ9Y"
consumer_secret="GJU9sIpmfXOOpS8XfEINE7v4qNzpUdyjaXKAyx2IUx3kJr5Y5k"

access_token="3220668044-sPijKWEAHCjPPFriNF5wqDuojrCrP7qZ4OcDyD1"
access_token_secret="4lBtGD08biSUrbMHlL6IIJ4yGyZ2zxZldjOGidqII6Ziz"

API_TOPIC = "api.twitter.pages.0522"
kafka = KafkaClient("172.31.10.154:9092")
producer = SimpleProducer(kafka)

class StdOutListener(StreamListener):

    def on_data(self, data):
        producer.send_messages(API_TOPIC, data)
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(languages=['en'],track=['news','media'])
