import queue
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
N = 100
print(dfhosts.head(N))
dfhosts = dfhosts.head(N)

async def fetch_warc(semaphore, s3client, row):
    url = row['url']
    warc_path = row['warc_filename']
    offset = int(row['warc_record_offset'])
    length = int(row['warc_record_length'])
    rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
    async with semaphore:
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

#Fetch all WARC records defined by filenames and offsets in rows, parse the records and the contained HTML, split the text into words and emit pairs <word, 1>
def processwarcrecords(dfhosts, writefiles, searchfiles, howmanyrecords):
    session = aioboto3.Session()
    
    async def analyzeDFRows(index, row, semaphore, db, s3client):
        async with semaphore:
            try:
                times3_start = time.perf_counter()
                body_data = await fetch_warc(semaphore, s3client, row)
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

    async def fetch_worker(fetch_queue, write_queue, index, semaphore, db, s3client):
        while True:
            row = await fetch_queue.get()
            if row is None:  # poison pill = shutdown signal
                break
            #await analyzeDFRows(index, row, semaphore, db, s3client)
            body_data = await fetch_warc(semaphore, s3client, row)
            tmplinks_list, tmpcomments_list, tmptitles_list = await asyncio.get_event_loop().run_in_executor(None, parse_html, body_data)

            await write_queue.put((tmplinks_list, tmpcomments_list, tmptitles_list))

            fetch_queue.task_done()
    
    async def write_worker(write_queue, db_lock, db):
        batch_links    = []
        batch_comments = []
        batch_titles   = []
        BATCH_SIZE = 100  # accumulate 100 records then write once
        while True:
            item = await write_queue.get()
            if item is None:  # poison pill = shutdown signal
                await flush_batch(db_lock, db, batch_links, batch_comments, batch_titles)  # flush any remaining items
                break

            tmplinks_list, tmpcomments_list, tmptitles_list = item
            batch_links.extend(tmplinks_list)
            batch_comments.extend(tmpcomments_list)
            batch_titles.extend(tmptitles_list)

            if len(batch_links) >= BATCH_SIZE:
                await flush_batch(db_lock, db, batch_links, batch_comments, batch_titles)
                batch_links.clear()
                batch_comments.clear()
                batch_titles.clear()

            # async with semaphore:
            #     await db.executemany('''INSERT INTO titles (url, title) VALUES (?, ?)''', tmptitles_list)
            #     await db.executemany('''INSERT INTO links (url, link) VALUES (?, ?)''', tmplinks_list)
            #     await db.executemany('''INSERT INTO comments (url, comment) VALUES (?, ?)''', tmpcomments_list) 
            #     await db.commit()
            write_queue.task_done()

    async def flush_batch(db_lock, db, links, comments, titles):
        async with db_lock:
            await db.executemany('''INSERT INTO links (url, link)   VALUES (?,?)''', links)
            await db.executemany('''INSERT INTO comments (url, comment) VALUES (?,?)''', comments)
            await db.executemany('''INSERT INTO titles   (url, title)   VALUES (?,?)''', titles)
            await db.commit()

    async def main():

        sem = asyncio.Semaphore(value=1000) # limit to 20 concurrent tasks to avoid overwhelming the system
        import time
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
                fetch_queue = asyncio.Queue(maxsize=1000)
                write_queue = asyncio.Queue(maxsize=1000)
                db_lock = asyncio.Lock()  # to serialize DB writes if needed, but we will batch to minimize contention
                NUM_WORKERS = 50
                fetch_workers = [
                    asyncio.create_task(fetch_worker(fetch_queue, write_queue, index, sem, db, s3client)) 
                    for index in range(NUM_WORKERS)
                ]
                write_workers = [
                    asyncio.create_task(write_worker(write_queue, db_lock, db)) 
                    for _ in range(3)
                ]

                for index, row in dfhosts.iterrows():
                    await fetch_queue.put(row)
                for _ in range(NUM_WORKERS):
                    await fetch_queue.put(None)
                await asyncio.gather(*fetch_workers)

                for _ in range(3):
                    await write_queue.put(None)
                await asyncio.gather(*write_workers)

        end = time.perf_counter()
        print(f"Total time: {end - start:.2f} seconds")

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
    asyncio.run(main())

searchfiles = 'yes' # anything other than 'yes' will not process
writefiles = 'no' # anything other than 'yes' will not process
howmanyrecords = 0 # 0 is all records; other options would be a numeric value
processwarcrecords(dfhosts,writefiles,searchfiles,howmanyrecords)
