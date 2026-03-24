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
dfhosts = dfhosts.head(10)

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
    async def analyzeDFRows(index, row, semaphore, db):
        # async with aiosqlite.connect('db_straylight.db') as db:
            async with session.client("s3") as s3client:
                # for index, row in dfhosts.iterrows():
                async with semaphore:
                    # await db.execute('''CREATE TABLE IF NOT EXISTS titles (url TEXT, title TEXT)''')
                    # await db.execute('''CREATE TABLE IF NOT EXISTS links (url TEXT, link TEXT)''')
                    # await db.execute('''CREATE TABLE IF NOT EXISTS comments (url TEXT, comment TEXT)''')
                    # if recordcount > howmanyrecords:
                    #     break
                    nonlocal recordcount, skippedrecords, processedrecords, processedrows
                    recordcount = recordcount + 1
                    processedrows = processedrows + 1
                    #print('Processing row ' + str(recordcount) + 
                    #    ' of ' + str(totalrecords) + ' total rows.')
                    #print('Processed ' + str(processedrecords) + ' records.')
                    url = row['url']
                    warc_path = row['warc_filename']
                    offset = int(row['warc_record_offset'])
                    length = int(row['warc_record_length'])
                    rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
                    times3_start = time.perf_counter()
                    response = await s3client.get_object(Bucket='commoncrawl',
                                                Key=warc_path,Range=rangereq, RequestPayer='requester')
                    times3_end = time.perf_counter()
                    print("s3 get time: %.2f" % (times3_end - times3_start))
                    body_data = await response['Body'].read()
                    record_stream = BytesIO(body_data)
                    for record in ArchiveIterator(record_stream):
                        tmptitles_list = []
                        tmplinks_list = []
                        tmpcomments_list = []
                        if record.rec_type == 'response':
                            try:
                                warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')

                                page = record.content_stream().read()
                                # lxml should be faster but is not
                                timebs_start = time.perf_counter()
                                soup = BeautifulSoup(page, 'html.parser') 
                                timebs_end = time.perf_counter()
                                print("time bs: %.2f" % (timebs_end - timebs_start))
                                title = soup.title.string
                                # titles_list.append((warc_target_uri, title))
                                # timedbins_start = time.perf_counter()
                                # await db.execute('''INSERT INTO titles (url, title) VALUES (?, ?)''', (warc_target_uri, title))
                                # await db.commit()
                                tmptitles_list.append((warc_target_uri, title))
                                # timedbins_end = time.perf_counter()
                                # print("time db insert: %.2f", (timedbins_end - timedbins_start))
                                # uris_list.append((warc_target_uri))
                                if searchfiles == 'yes':
                                    # Find all links
                                    timelinks_start = time.perf_counter()
                                    for link in soup.find_all('a'):
                                        # links_list.append((warc_target_uri, link.get('href')))
                                        # await db.execute('''INSERT INTO links (url, link) VALUES (?, ?)''', (warc_target_uri, link.get('href')))
                                        # await db.commit()
                                        tmplinks_list.append((warc_target_uri, link.get('href')))
                                    timelinks_end = time.perf_counter()
                                    print("time links: %.2f" % (timelinks_end - timelinks_start))
                                    # Find all comments
                                    timecomments_start = time.perf_counter()
                                    for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
                                        # comments_list.append((warc_target_uri, comment))
                                        # await db.execute('''INSERT INTO comments (url, comment) VALUES (?, ?)''', (warc_target_uri, comment))
                                        # await db.commit()
                                        tmpcomments_list.append((warc_target_uri, comment))
                                    timecomments_end = time.perf_counter()
                                    print("time comments: %.2f" % (timecomments_end - timecomments_start))
                                #print('Found title: ' + title)
                                #print('Found ' + str(len(links_list)) + ' links so far.')
                                #print('Found ' + str(len(comments_list)) + ' comments so far.')
                                timedbexecmany_start = time.perf_counter()
                                await db.executemany('''INSERT INTO titles (url, title) VALUES (?, ?)''', tmptitles_list)
                                await db.executemany('''INSERT INTO links (url, link) VALUES (?, ?)''', tmplinks_list)
                                await db.executemany('''INSERT INTO comments (url, comment) VALUES (?, ?)''', tmpcomments_list) 
                                await db.commit()
                                timedbexecmany_end = time.perf_counter()
                                print("time db executemany: %.2f" % (timedbexecmany_end - timedbexecmany_start))
                                if writefiles == 'yes':
                                    page = page.decode("utf-8") 
                                    url = url.replace("https://","")
                                    url = url.replace("http://","")
                                    url = url + str(offset) + '.html'
                                    filepath = os.getcwd() + '/tmp/' + url
                                    os.makedirs(os.path.dirname(filepath), exist_ok=True)
                                    with open(filepath, "w") as text_file:
                                        text_file.write(soup.prettify())
                                        processedrecords = processedrecords + 1
                            except Exception as e:
                                logger = logging.getLogger('errorhandler')
                                print(logger.error('Error: '+ str(e)))
                                skippedrecords = skippedrecords + 1
                                print('Skipped ' + str(skippedrecords) + ' records.')

    async def main():

        sem = asyncio.Semaphore(value=1000) # limit to 20 concurrent tasks to avoid overwhelming the system
        import time
        nonlocal processedrows
        async with aiosqlite.connect('db_straylight.db') as db:
            # Set this once when opening the DB
            await db.execute('PRAGMA journal_mode=WAL')
            await db.execute('PRAGMA synchronous=NORMAL')
            await db.execute('PRAGMA cache_size=10000')
            await db.execute('''CREATE TABLE IF NOT EXISTS titles (url TEXT, title TEXT)''')
            await db.execute('''CREATE TABLE IF NOT EXISTS links (url TEXT, link TEXT)''')
            await db.execute('''CREATE TABLE IF NOT EXISTS comments (url TEXT, comment TEXT)''')
            await db.commit()
            # tasks = [analyzeDFRows(index, row, sem, db) for (index, row) in dfhosts.iterrows()]
            # await asyncio.gather(*tasks)
            start = time.perf_counter()
            totalrows = len(dfhosts.index)
            BATCH_SIZE = 1000
            for i in range(0, totalrows, BATCH_SIZE):
                batch = dfhosts.iloc[i:i+BATCH_SIZE]
                tasks = [analyzeDFRows(index, row, sem, db) for (index, row) in batch.iterrows()]
                await asyncio.gather(*tasks)
            # for index, row in dfhosts.iterrows():
            #     print('doing index: ', index)
            #     await analyzeDFRows(index, row, sem, db)

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
