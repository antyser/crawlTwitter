__author__ = 'junliu'
from pykafka import KafkaClient


def create_producer(kafka_host, topic):
    client = KafkaClient(hosts=kafka_host)
    producer_topic = client.topics[topic]
    producer = producer_topic.get_producer()
    return producer


def create_consumer(kafka_host, zookeeper_host, topic, consumer_group):
    client = KafkaClient(hosts=kafka_host)
    consumer_topic = client.topics[topic]
    consumer = consumer_topic.get_balanced_consumer(
        consumer_group=consumer_group,
        auto_commit_enable=True,
        zookeeper_connect=zookeeper_host)
    return consumer
