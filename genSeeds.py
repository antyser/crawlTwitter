__author__ = 'junliu'
from kafka import SimpleProducer, KafkaClient
import json
import sys
import yaml

def load_config():
    with open('config.yml', 'r') as fl:
        cnf = yaml.load(fl)
        return cnf


if __name__ == '__main__':
    print 'usage: python genSeeds.py'
    kafka_host = '172.31.10.154:9092'
    if len(sys.argv) == 2:
        kafka_host = sys.argv[1]
    kafka = KafkaClient(kafka_host)
    cfg = load_config()
    producer = SimpleProducer(kafka)
    print 'connected to kafka, start populating seeds'

    INITIAL_URL = "https://twitter.com/{0}"
    with open(cfg['seed_list'], "r") as indoc:
        for line in indoc:
            seed = dict()
            seed['url'] = INITIAL_URL.format(line.strip())
            producer.send_messages(cfg['kafka']['seeds'], json.dumps(seed))
    kafka.close()