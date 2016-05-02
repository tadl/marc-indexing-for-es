#!/usr/bin/env python

import configparser
import logging
import logging.config
import sys
from datetime import date
from elasticsearch import Elasticsearch

logging.config.fileConfig('index-config.ini')

config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

index_base_name = config['elasticsearch']['index']

today = date.today()
today_string = today.strftime("%Y%m%d")

index_name = index_base_name + '_' + today_string

if (es.ping()):
    print("ping!")

if es.indices.exists_alias(name=index_base_name):
    alias = es.indices.get_alias(name=index_base_name)
    current_index = alias.keys()[0]
    logging.info(repr(alias))
    logging.info(current_index)
    es.indices.update_aliases(body={
        'actions': [
                    {'remove': {'index': current_index, 'alias': index_base_name}},
                    {'add': {'index': index_name, 'alias': index_base_name}},
                   ]})
else:
    logging.info("alias %s does not exist?" % (index_base_name,))
    es.indices.put_alias(index=index_name, name=index_base_name)
