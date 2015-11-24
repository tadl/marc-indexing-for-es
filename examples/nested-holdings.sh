#!/bin/bash

. load_config.sh

# This overrides the index name to be one that may not exist.
ES_INDEX="records-v9"

# This searches for an item with a specific barcode at a specific lib,
# and is expected to return a single record even with non-nested holdings

# For a real test, change the barcode to match an item on that record
# that has a circ_lib that is NOT TADL-PCL, and you'll see a difference:
#  - in a nested holdings index you'll get zero results, because
#    no single holding matched the (intentionally impossible) criteria
#  - in a non-nested index you'll get a single record as long as the
#    record has a holding with the given barcode AND has any holding
#    with the given circ lib

curl -XPOST "$ES_URL/$ES_INDEX/_search?pretty=true" -d '
{"query":
    {
    "filtered" : {
        "query" : { "match_all" : {} },
        "filter" : {
            "nested" : {
                "path" : "holdings",
                "filter" : {
                    "bool" : {
                        "must" : [
                            {
                                "term" : {"holdings.barcode" : "4027694"}
                            },
                            {
                                "term" : {"holdings.circ_lib" : "TADL-PCL"}
                            }
                        ]
                    }
                },
                "_cache" : true
            }
        }
    }
}
}'
