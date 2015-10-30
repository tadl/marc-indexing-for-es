Examples
========

You will need to copy config-example.sh to config.sh and adjust the
settings within.

Examples pass ?pretty as an argument to tell Elasticsearch to
pretty-print JSON output.

Piping the output through jq can add color and even more
pretty-printing, as well as allow for filtering.

    # No filtering, just pretty-print and auto-color:
    ./available.sh | jq .

    # No filtering, pipe through less and preserve color:
    ./available.sh | jq -C . | less -R
