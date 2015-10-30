#!/bin/bash

. load_config.sh

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
    "query": {
        "match": {"_all": "apples"}
    },
    "aggs": {
        "genres": {
            "terms": {
                "field": "genres.raw"
            }
        },
        "authors": {
            "terms": {
                "field": "author.raw"
            }
        },
        "availability": {
            "terms": {
                "field": "holdings.status"
            }
        },
        "type_of_resource": {
            "terms": {
                "field": "type_of_resource"
            }
        }
    }
}'
