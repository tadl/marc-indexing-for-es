#!/usr/bin/env python

import configparser

from elasticsearch import Elasticsearch

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
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed",
                            "include_in_all": "false",
                        },
                    },
                },
                'genres': {
                    'type': 'string',
                    'fields': {
                        "raw": {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                },
                'series': {
                    'type': 'string',
                    'fields': {
                        "raw": {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                },
                'subjects': {
                    'type': 'string',
                    'fields': {
                        "raw": {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                },
                'author': {
                    'type': 'string',
                    'fields': {
                        "raw": {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                },
                'corpauthor': {
                    'type': 'string',
                    'fields': {
                        "raw": {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                },
                'type_of_resource': {
                    'type': 'string',
                    'index': 'not_analyzed',
                    'include_in_all': 'false',
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
                        'circ_lib': {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                        'call_number': {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                        'location': {
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
