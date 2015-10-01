#!/usr/bin/env python

import logging, logging.config, configparser

from elasticsearch import Elasticsearch

logging.config.fileConfig('index-config.ini')
config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

index_name = config['elasticsearch']['index']

if (es.ping()):
    print "ping!"

es.indices.create(
    index=index_name,
    body={
        'settings': {
            'number_of_shards': 5,
            'number_of_replicas': 1,
            'analysis': {
                'analyzer': {
                    'folding': {
                        'filter': ['lowercase', 'asciifolding'],
                        'tokenizer': 'standard',
                    },
                },
            },
        },
    }
)

es.indices.put_mapping(
    index=index_name,
    doc_type='record',
    body={
        'record': {
            'properties': {
                'title': {
                    'type': 'string',
                    'analyzer': 'english',
                    'fields': {
                        "folded": {
                            "type": "string",
                            "analyzer": "folding",
                        },
                    },
                },
                'holdings': {
                    'properties': {
                        'barcode': {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                        'status': {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                }
            }
        }
    }
)
