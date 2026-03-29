import time
import warcio
from warcio.archiveiterator import ArchiveIterator
import os
from bs4 import BeautifulSoup 
from bs4 import Comment
import io
from io import BytesIO
import logging
# import dask.dataframe as ddf
import multiprocessing
import asyncio

import os
import csv
import aioboto3
import pandas as pd
import threading

from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup, Comment

import aiosqlite

import os

from botocore import UNSIGNED
from botocore.config import Config

if os.path.exists('db_straylight.db'):
    os.remove('db_straylight.db')

dfhosts = pd.read_csv('./athena-9ecf898d-29ee-4f25-a188-f6581aa993a1.csv')
print(dfhosts.head(10))
dfhosts = dfhosts.head(1)

async def fetch_warc(s3client, row):
    url = row['url']
    warc_path = row['warc_filename']
    offset = int(row['warc_record_offset'])
    length = int(row['warc_record_length'])
    rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
    response = await s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq, RequestPayer='requester')
    body_data = await response['Body'].read()
    return body_data

def parse_html(data):
    record_stream = BytesIO(data)
    titles = []
    for record in ArchiveIterator(record_stream):
        if record.rec_type == 'response':
            warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')
            page = record.content_stream().read()
            soup = BeautifulSoup(page, 'html.parser') 
            title = soup.title.string if soup.title else ''
            titles.append((warc_target_uri, title))
            links = [(warc_target_uri, link.get('href')) for link in soup.find_all('a')]
            comments = [(warc_target_uri, comment) for comment in soup.find_all(text=lambda text: isinstance(text, Comment))]
            return links, comments, titles
    return [], [], []

# Thank you to Sebastian Nagel for your instructions and code to perform the following step.
# http://netpreserve.org/ga2019/wp-content/uploads/2019/07/IIPCWAC2019-SEBASTIAN_NAGEL-Accessing_WARC_files_via_SQL-poster.pdf
titles_list = []
uris_list = []
links_list = []
comments_list = []

#Fetch all WARC records defined by filenames and offsets in rows, parse the records and the contained HTML, split the text into words and emit pairs <word, 1>
def processwarcrecords(dfhosts, writefiles, searchfiles, howmanyrecords):
    session = aioboto3.Session()
    
    # s3client = boto3.client('s3', endpoint_url="http://localhost:5000")
    processedrows = 0
    recordcount = 0
    skippedrecords = 0
    processedrecords = 0
    totalrecords = len(dfhosts.index)
    if howmanyrecords == 0:
        howmanyrecords = totalrecords

    async def analyzeDFRows(index, row, semaphore, db, s3client):
        async with semaphore:
            try:
                nonlocal recordcount, skippedrecords, processedrecords, processedrows
                recordcount = recordcount + 1
                processedrows = processedrows + 1
                times3_start = time.perf_counter()
                body_data = await fetch_warc(s3client, row)
                times3_end = time.perf_counter()
                print("s3 get time: %.2f" % (times3_end - times3_start))
                tmplinks_list, tmpcomments_list, tmptitles_list = await asyncio.get_event_loop().run_in_executor(None, parse_html, body_data)
                print("waiting for db executemany...   ")
                timedbexecmany_start = time.perf_counter()
                await db.executemany('''INSERT INTO titles (url, title) VALUES (?, ?)''', tmptitles_list)
                await db.executemany('''INSERT INTO links (url, link) VALUES (?, ?)''', tmplinks_list)
                await db.executemany('''INSERT INTO comments (url, comment) VALUES (?, ?)''', tmpcomments_list) 
                await db.commit()
                timedbexecmany_end = time.perf_counter()
                print("time db executemany: %.2f" % (timedbexecmany_end - timedbexecmany_start))
            except Exception as e:
                logger = logging.getLogger('errorhandler')
                print(logger.error('Error: '+ str(e)))
                skippedrecords = skippedrecords + 1
                print('Skipped ' + str(skippedrecords) + ' records.')

    async def worker(queue, index, semaphore, db, s3client):
        while True:
            row = await queue.get()
            if row is None:  # poison pill = shutdown signal
                break
            await analyzeDFRows(index, row, semaphore, db, s3client)
            queue.task_done()

    async def main():

        sem = asyncio.Semaphore(value=1000) # limit to 20 concurrent tasks to avoid overwhelming the system
        import time
        nonlocal processedrows
        async with aiosqlite.connect('db_straylight.db') as db:

            async with session.client("s3") as s3client:
                # Set this once when opening the DB
                await db.execute('PRAGMA journal_mode=WAL')
                await db.execute('PRAGMA synchronous=NORMAL')
                await db.execute('PRAGMA cache_size=10000')
                await db.execute('''CREATE TABLE IF NOT EXISTS titles (url TEXT, title TEXT)''')
                await db.execute('''CREATE TABLE IF NOT EXISTS links (url TEXT, link TEXT)''')
                await db.execute('''CREATE TABLE IF NOT EXISTS comments (url TEXT, comment TEXT)''')
                await db.commit()
                start = time.perf_counter()

                # - - - - - using a queue and workers - - - - -
                queue = asyncio.Queue(maxsize=1000)
                NUM_WORKERS = 50
                workers = [
                    asyncio.create_task(worker(queue, index, sem, db, s3client)) 
                    for index in range(NUM_WORKERS)
                ]
                for index, row in dfhosts.iterrows():
                    await queue.put(row)
                for _ in range(NUM_WORKERS):
                    await queue.put(None)
                await asyncio.gather(*workers)

        end = time.perf_counter()

        async with aiosqlite.connect('db_straylight.db') as db:
            # await db.commit()
            async with db.execute("SELECT * FROM titles") as cursor:
                print("\nTitles:")
                async for row in cursor:
                    print(row)
                    break
            async with db.execute("SELECT * FROM links") as cursor:
                print("\nLinks:")
                async for row in cursor:
                    print(row)
                    break
            async with db.execute("SELECT * FROM comments") as cursor:
                print("\nComments:")
                async for row in cursor:
                    print(row)
                    break
        print(f"\nProcessed {processedrecords} records, skipped {skippedrecords} records in {end - start:0.2f} seconds.")
    asyncio.run(main())

searchfiles = 'yes' # anything other than 'yes' will not process
writefiles = 'no' # anything other than 'yes' will not process
howmanyrecords = 0 # 0 is all records; other options would be a numeric value
processwarcrecords(dfhosts,writefiles,searchfiles,howmanyrecords)
