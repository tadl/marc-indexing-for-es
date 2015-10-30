#!/bin/bash

. load_config.sh

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
    "query": {
        "match_all": { }
    },
    "aggs": {
        "type_of_resource": {
            "terms": {
                "field": "type_of_resource"
            }
        }
    }
}'
