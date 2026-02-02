#!/bin/bash
# collects all urls from all the indices on common crawl
# saves them to a file for a particular domain
curl -s https://index.commoncrawl.org/collinfo.json \
| jq -r '.[].id' \
| while read index; do
    sleep 3s
    echo "[*] Querying $index" >&2
    curl -s "https://index.commoncrawl.org/$index-index?url=example.com&matchType=domain&output=json"
done \
| jq -r '.url' \
| sort -u \
> underarmour_all_urls.txt

