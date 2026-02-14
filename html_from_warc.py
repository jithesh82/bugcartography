from warcio.archiveiterator import ArchiveIterator
import gzip

target = "https://www.underarmour.com/en-us/c/mens/sports/golf/green+purple+yellow-clothing/"

with gzip.open("CC-MAIN-20240304133241-20240304163241-00450.warc.gz", "rb") as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == "response":
            if record.rec_headers.get_header("WARC-Target-URI") == target:
                payload = record.content_stream().read()
                print(payload.decode("utf-8", errors="ignore"))
                with open('testhtml.html', 'w') as f:
                    f.write(payload.decode("utf-8", errors="ignore"))
