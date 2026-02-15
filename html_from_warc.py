from warcio.archiveiterator import ArchiveIterator
import gzip

#target = "https://www.underarmour.com/en-us/c/mens/sports/golf/green+purple+yellow-clothing/"
target = "http://www.underarmour.com/"

with gzip.open("CC-MAIN-20260114060053-20260114090053-00556.warc.gz", "rb") as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == "response":
            #if record.rec_headers.get_header("WARC-Target-URI") == target:
                payload = record.content_stream().read()
                print(payload.decode("utf-8", errors="ignore"))
                with open('CC-MAIN-20260114060053-20260114090053-00556.warc.gz.html', 'w') as f:
                    f.write(payload.decode("utf-8", errors="ignore"))
