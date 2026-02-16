"""
save the html from the warc file
"""
from warcio.archiveiterator import ArchiveIterator
import gzip
import os

#target = "https://www.underarmour.com/en-us/c/mens/sports/golf/green+purple+yellow-clothing/"
target = "http://www.underarmour.com/"

for warcF in os.listdir("./warcdir"):
    with gzip.open("./warcdir/"+warcF, "rb") as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                print(record.rec_headers.get_header("WARC-Target-URI"))
                http_headers = record.http_headers
                print(http_headers)
                statuscode = http_headers.get_statuscode()
                print(f"HTTP Status: {statuscode}")
                uri = record.rec_headers.get('WARC-Target-URI')
                print(uri)
                warcFID = warcF.split('.')[0]
                if statuscode[0] in ['4', '5']:
                    outputFname = warcFID + '.txt'
                    with open('./html/' + outputFname, 'w') as f:
                        f.write(uri + '\n')
                        f.write(str(http_headers))
                else:
                    payload = record.content_stream().read()
                    print(payload.decode("utf-8", errors="ignore"))
                    outputFname = warcFID +'.html'
                    with open('./html/'+outputFname, 'w') as f:
                        f.write(uri + '\n')
                        f.write(payload.decode("utf-8", errors="ignore"))
