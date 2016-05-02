#!/usr/bin/env python

import configparser

from elasticsearch import Elasticsearch

config = configparser.ConfigParser()
config.read('index-config.ini')

es = Elasticsearch([config['elasticsearch']['url']])

index_name = config['elasticsearch']['index']

if (es.ping()):
    print("ping!")

es.indices.create(
    index=index_name,
    body={
        'settings': {
            'number_of_shards': 5,
            'number_of_replicas': 1,
            'analysis': {
                'analyzer': {
                    'ducet_sort': {
                        'tokenizer': 'keyword',
                        'filter': 'icu_collation',
                    },
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
                "source": {
                    "type": "string",
                    "index": "not_analyzed",
                    "include_in_all": "false",
                },
                'title_display': {
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
                'title': {
                    'type': 'string',
                    'analyzer': 'english',
                    'include_in_all': 'false',
                    'fields': {
                        "folded": {
                            "type": "string",
                            "analyzer": "folding",
                            'include_in_all': 'false',
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed",
                            "include_in_all": "false",
                        },
                    },
                },
                'title_alt': {
                    'type': 'string',
                    'analyzer': 'english',
                    'include_in_all': 'false',
                    'fields': {
                        "folded": {
                            "type": "string",
                            "analyzer": "folding",
                            'include_in_all': 'false',
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed",
                            "include_in_all": "false",
                        },
                    },
                },
                'title_short': {
                    'type': 'string',
                    'analyzer': 'english',
                    'include_in_all': 'false',
                    'fields': {
                        "folded": {
                            "type": "string",
                            "analyzer": "folding",
                            'include_in_all': 'false',
                        },
                        "raw": {
                            "type": "string",
                            "index": "not_analyzed",
                            "include_in_all": "false",
                        },
                    },
                },
                'title_nonfiling': {
                    'type': 'string',
                    'analyzer': 'english',
                    'include_in_all': 'false',
                    'fields': {
                        'folded': {
                            'type': 'string',
                            'analyzer': 'folding',
                            'include_in_all': 'false',
                        },
                        'sort': {
                            'type': 'string',
                            'analyzer': 'ducet_sort',
                            'include_in_all': 'false',
                        },
                    },
                },
                'sort_year': {
                        "type": "integer",
                        "index": "not_analyzed",
                        "include_in_all": "false",
                },
                'genres': {
                    'type': 'string',
                    'norms': { 'enabled': 'false' },
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
                    'norms': { 'enabled': 'false' },
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
                'author_other': {
                    'type': 'string',
                    'norms': { 'enabled': 'false' },
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
                'electronic': {
                    'type': 'boolean',
                    'index': 'not_analyzed',
                    'include_in_all': 'false',
                },
                'holdings': {
                    'type': 'nested',
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
                        'location_id': {
                            'type': 'integer',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                        'location': {
                            'type': 'string',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                        'due_date': {
                            'type': 'date',
                            'format': 'yyyy-MM-dd',
                            'index': 'not_analyzed',
                            'include_in_all': 'false',
                        },
                    }
                }
            }
        }
    }
)
