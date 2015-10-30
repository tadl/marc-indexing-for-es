#!/bin/bash

. load_config.sh

# See https://www.elastic.co/guide/en/elasticsearch/reference/1.7/search-request-sort.html

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{
    "sort": [
        { "title.raw": "asc" },
        { "author.raw": "asc" },
        "_score"
    ],
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
