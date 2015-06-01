__author__ = 'junliu'
import json
import sys,os
import yaml
import kafkaUtil


def load_config():
    with open(os.path.join(sys.path[0], 'config.yml'), 'r') as fl:
        cnf = yaml.load(fl)
        return cnf



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print 'usage: python genSeeds.py <kafka_host>'
    else:
        kafka_host = sys.argv[1]
        cfg = load_config()
        producer = kafkaUtil.create_producer(kafka_host, cfg['kafka']['seeds'])
        print 'connected to kafka, start populating seeds'

        INITIAL_URL = "https://twitter.com/{0}"
        with open(cfg['seed_list'], "r") as indoc:
            for line in indoc:
                seed = dict()
                seed['url'] = INITIAL_URL.format(line.strip())
                print 'input:', seed
                producer.produce([json.dumps(seed)])
