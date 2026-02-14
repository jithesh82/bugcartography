# coding: utf-8
from warcio.archiveiterator import ArchiveIterator
target = "www.underarmour.com"
with open('1213890397422_13.arc', 'rb') as f:
    for record in ArchiveIterator(f):
        print(record.rec_type)
        print(record.rec_headers.get_header("WARC-Target-URI"))
        print(record.rec_headers.get_header("uri"))

        with open('1213890397422_13.html', 'w') as html:
            html.write(record.content_stream().read().decode())
