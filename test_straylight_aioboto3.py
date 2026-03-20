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

#!/usr/bin/env python3
"""
Local test harness for processwarcrecords.py
Uses moto to simulate AWS S3 locally - no real AWS account needed.

What it does:
  1. Spins up a fake S3 bucket called 'commoncrawl'
  2. Creates realistic fake WARC files with HTML pages for your domain
  3. Builds a fake dfhosts.csv with correct offsets pointing into those WARCs
  4. Runs the WARC processor against the fake data
  5. Outputs results to dftitles.csv, dflinks.csv, dfcomments.csv

Usage:
  python3 test_local.py
"""

import os
import csv
import boto3
import aioboto3
import pandas as pd
import threading
import tqdm

from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import patch

# ── moto must be imported and started BEFORE boto3 clients are created ─────────
from moto import mock_aws

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup, Comment

import aiosqlite

import os

if os.path.exists('db_straylight.db'):
    os.remove('db_straylight.db')

# ─── FAKE PAGE TEMPLATES ───────────────────────────────────────────────────────
# Simulates realistic pages a bug bounty hunter would care about
FAKE_PAGES = [
    {
        "url": "http://example.com/",
        "title": "Example Home Page",
        "html": """<html><head><title>Example Home Page</title></head>
        <body>
          <!-- internal build v1.2.3 -->
          <a href="/login">Login</a>
          <a href="/admin">Admin Panel</a>
          <a href="/api/v1/users">API Users</a>
        </body></html>"""
    },
    {
        "url": "http://example.com/login",
        "title": "Login Page",
        "html": """<html><head><title>Login Page</title></head>
        <body>
          <!-- TODO: remove debug=true before prod -->
          <a href="/reset-password?token=abc123">Reset Password</a>
          <a href="/oauth?redirect=http://example.com">OAuth Login</a>
        </body></html>"""
    },
    {
        "url": "http://example.com/admin",
        "title": "Admin Dashboard",
        "html": """<html><head><title>Admin Dashboard</title></head>
        <body>
          <!-- admin secret key: DONT_COMMIT_THIS -->
          <a href="/admin/users">Manage Users</a>
          <a href="/admin/export?format=csv">Export Data</a>
          <a href="/admin/debug">Debug Console</a>
        </body></html>"""
    },
    {
        "url": "http://example.com/api/v1/users",
        "title": "API Endpoint",
        "html": """<html><head><title>API Endpoint</title></head>
        <body>
          <!-- jwt secret: supersecretkey123 -->
          <a href="/api/v1/users?id=1">User 1</a>
          <a href="/api/v1/users?id=2">User 2</a>
        </body></html>"""
    },
    {
        "url": "http://example.com/upload",
        "title": "File Upload",
        "html": """<html><head><title>File Upload</title></head>
        <body>
          <!-- cookie: session=eyJhbGciOiJIUzI1NiJ9 -->
          <a href="/upload?path=/etc/passwd">Upload</a>
          <a href="/files?file=../../config.php">Config</a>
        </body></html>"""
    },
]

# ─── BUILD FAKE WARC FILE ──────────────────────────────────────────────────────
def build_fake_warc(pages):
    """
    Creates a WARC file in memory containing all fake pages.
    Returns the raw bytes and a list of (url, offset, length) for each record.
    """
    output = BytesIO()
    writer = WARCWriter(output, gzip=False)
    offsets = []

    for page in pages:
        # Record offset before writing
        offset = output.tell()

        html_bytes = page["html"].encode("utf-8")
        http_response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/html; charset=utf-8\r\n"
            b"\r\n"
        ) + html_bytes

        record = writer.create_warc_record(
            page["url"],
            "response",
            payload=BytesIO(http_response),
            http_headers=StatusAndHeaders("200 OK", [], protocol="HTTP/1.1"),
        )
        writer.write_record(record)

        length = output.tell() - offset
        offsets.append((page["url"], offset, length))

    return output.getvalue(), offsets


# ─── BUILD FAKE dfhosts.csv ────────────────────────────────────────────────────
def build_fake_dfhosts(offsets, warc_key):
    rows = []
    for url, offset, length in offsets:
        rows.append({
            "url":                url,
            "url_query":          "",
            "warc_filename":      warc_key,
            "warc_record_offset": offset,
            "warc_record_length": length,
            "fetch_time":         "2023-01-01 00:00:00",
        })
    df = pd.DataFrame(rows)
    # crete a a large dfhosts.csv file to test performance with many records by
    # concatenating the existing rows multiple times
    df = pd.concat([df] * 1000, ignore_index=True)
    df.to_csv("dfhosts.csv", index=False)
    print(f"  Created dfhosts.csv with {len(df)} rows")
    return df

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def createS3Data():
    global processed, skipped

    print("=" * 60)
    print("  Common Crawl Local Test Harness (moto S3 mock)")
    print("=" * 60)

    # 1. Create fake S3 bucket
    print("\n[1] Setting up fake S3 bucket...")
    #s3 = boto3.client("s3", region_name="us-east-1")
    s3 = boto3.client("s3", endpoint_url="http://localhost:5000")
    s3.create_bucket(Bucket="commoncrawl")
    print("  Created fake bucket: commoncrawl")
    #import pdb
    #pdb.set_trace()

    # 2. Build and upload fake WARC
    print("\n[2] Building fake WARC file with test pages...")
    warc_bytes, offsets = build_fake_warc(FAKE_PAGES)
    warc_key = "crawl-data/CC-MAIN-2023-test/segments/test/warc/test.warc"
    s3.put_object(Bucket="commoncrawl", Key=warc_key, Body=warc_bytes)
    print(f"  Uploaded fake WARC: {len(warc_bytes)} bytes, {len(offsets)} records")

    # 3. Build fake dfhosts.csv
    print("\n[3] Building fake dfhosts.csv...")
    dfhosts = build_fake_dfhosts(offsets, warc_key)

    return dfhosts

dfhosts = createS3Data()
# import pdb
# pdb.set_trace()

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
    recordcount = 0
    skippedrecords = 0
    processedrecords = 0
    totalrecords = len(dfhosts.index)
    if howmanyrecords == 0:
        howmanyrecords = totalrecords
    async def analyzeDFRows(index, row, semaphore):
        async with aiosqlite.connect('db_straylight.db') as db:
            async with session.client("s3", endpoint_url="http://localhost:5000") as s3client:
                # for index, row in dfhosts.iterrows():
                async with semaphore:
                    await db.execute('''CREATE TABLE IF NOT EXISTS titles (url TEXT, title TEXT)''')
                    await db.execute('''CREATE TABLE IF NOT EXISTS links (url TEXT, link TEXT)''')
                    await db.execute('''CREATE TABLE IF NOT EXISTS comments (url TEXT, comment TEXT)''')
                    # if recordcount > howmanyrecords:
                    #     break
                    nonlocal recordcount, skippedrecords, processedrecords
                    recordcount = recordcount + 1
                    #print('Processing row ' + str(recordcount) + 
                    #    ' of ' + str(totalrecords) + ' total rows.')
                    #print('Processed ' + str(processedrecords) + ' records.')
                    url = row['url']
                    warc_path = row['warc_filename']
                    offset = int(row['warc_record_offset'])
                    length = int(row['warc_record_length'])
                    rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
                    response = await s3client.get_object(Bucket='commoncrawl',
                                                Key=warc_path,Range=rangereq)
                    body_data = await response['Body'].read()
                    record_stream = BytesIO(body_data)
                    for record in ArchiveIterator(record_stream):
                        if record.rec_type == 'response':
                            try:
                                warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')

                                page = record.content_stream().read()
                                # lxml should be faster but is not
                                soup = BeautifulSoup(page, 'html.parser') 
                                title = soup.title.string
                                # titles_list.append((warc_target_uri, title))
                                await db.execute('''INSERT INTO titles (url, title) VALUES (?, ?)''', (warc_target_uri, title))
                                await db.commit()
                                # uris_list.append((warc_target_uri))
                                if searchfiles == 'yes':
                                    # Find all links
                                    for link in soup.find_all('a'):
                                        # links_list.append((warc_target_uri, link.get('href')))
                                        await db.execute('''INSERT INTO links (url, link) VALUES (?, ?)''', (warc_target_uri, link.get('href')))
                                        await db.commit()
                                    # Find all comments
                                    for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
                                        # comments_list.append((warc_target_uri, comment))
                                        await db.execute('''INSERT INTO comments (url, comment) VALUES (?, ?)''', (warc_target_uri, comment))
                                        await db.commit()
                                #print('Found title: ' + title)
                                #print('Found ' + str(len(links_list)) + ' links so far.')
                                #print('Found ' + str(len(comments_list)) + ' comments so far.')
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

        sem = asyncio.Semaphore(value=50) # limit to 20 concurrent tasks to avoid overwhelming the system
        import time
        start = time.perf_counter()
        tasks = [analyzeDFRows(index, row, sem) for (index, row) in dfhosts.iterrows()]
        await asyncio.gather(*tasks)

        end = time.perf_counter()

        async with aiosqlite.connect('db_straylight.db') as db:
            # await db.commit()
            async with db.execute("SELECT * FROM titles") as cursor:
                print("\nTitles:")
                async for row in cursor:
                    print(row)
            async with db.execute("SELECT * FROM links") as cursor:
                print("\nLinks:")
                async for row in cursor:
                    print(row)
            async with db.execute("SELECT * FROM comments") as cursor:
                print("\nComments:")
                async for row in cursor:
                    print(row)
        print(f"\nProcessed {processedrecords} records, skipped {skippedrecords} records in {end - start:0.2f} seconds.")
    asyncio.run(main())

searchfiles = 'yes' # anything other than 'yes' will not process
writefiles = 'no' # anything other than 'yes' will not process
howmanyrecords = 0 # 0 is all records; other options would be a numeric value
processwarcrecords(dfhosts,writefiles,searchfiles,howmanyrecords)