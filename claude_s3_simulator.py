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
    df.to_csv("dfhosts.csv", index=False)
    print(f"  Created dfhosts.csv with {len(df)} rows")
    return df


# ─── PROCESSOR (mirrors processwarcrecords.py) ────────────────────────────────
lock      = threading.Lock()
processed = 0
skipped   = 0

def process_single_row(row, links_writer, comments_writer, titles_writer):
    global processed, skipped

    # boto3 will automatically use the moto mock since we're inside @mock_aws
    #s3client = boto3.client("s3", region_name="us-east-1")
    s3client = boto3.client("s3", endpoint_url="http://localhost:5000")

    url       = row["url"]
    warc_path = row["warc_filename"]
    offset    = int(row["warc_record_offset"])
    length    = int(row["warc_record_length"])
    rangereq  = "bytes={}-{}".format(offset, offset + length - 1)

    try:
        response      = s3client.get_object(Bucket="commoncrawl", Key=warc_path, Range=rangereq)
        record_stream = BytesIO(response["Body"].read())

        for record in ArchiveIterator(record_stream):
            if record.rec_type == "response":
                warc_target_uri = record.rec_headers.get_header("WARC-Target-URI")
                page            = record.content_stream().read()
                soup            = BeautifulSoup(page, "html.parser")

                local_titles   = []
                local_links    = []
                local_comments = []

                title = soup.title.string if soup.title else None
                local_titles.append([warc_target_uri, title])

                for link in soup.find_all("a"):
                    local_links.append([warc_target_uri, link.get("href")])

                for comment in soup.find_all(
                    text=lambda text: isinstance(text, Comment)
                ):
                    local_comments.append([warc_target_uri, str(comment).strip()])

                with lock:
                    titles_writer.writerows(local_titles)
                    links_writer.writerows(local_links)
                    comments_writer.writerows(local_comments)
                    processed += 1

    except Exception as e:
        with lock:
            skipped += 1
        print(f"  [ERROR] {e}")


# ─── MAIN ──────────────────────────────────────────────────────────────────────
#@mock_aws
def main():
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

    # 4. Run the processor
    print(f"\n[4] Processing {len(dfhosts)} WARC records...")
    rows = [row for _, row in dfhosts.iterrows()]

    with open("dftitles.csv",   "w", newline="", encoding="utf-8") as tf, \
         open("dflinks.csv",    "w", newline="", encoding="utf-8") as lf, \
         open("dfcomments.csv", "w", newline="", encoding="utf-8") as cf:

        titles_writer   = csv.writer(tf)
        links_writer    = csv.writer(lf)
        comments_writer = csv.writer(cf)

        titles_writer.writerow(["URI", "Title"])
        links_writer.writerow(["URI", "Link"])
        comments_writer.writerow(["URI", "Comment"])

        # Use only 3 threads for local test
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(process_single_row, row, links_writer, comments_writer, titles_writer)
                for row in rows
            ]
            with tqdm.tqdm(total=len(rows), desc="  Processing") as pbar:
                for future in as_completed(futures):
                    pbar.update(1)

    # 5. Show results
    print(f"\n[5] Results:")
    print(f"  Processed : {processed}")
    print(f"  Skipped   : {skipped}")
    print()

    for fname in ["dftitles.csv", "dflinks.csv", "dfcomments.csv"]:
        df = pd.read_csv(fname)
        print(f"  {fname}: {len(df)} rows")
        print(df.to_string(index=False))
        print()

    # 6. Keyword search on comments (mirrors the notebook)
    print("[6] Keyword search on comments:")
    import re
    df_comments = pd.read_csv("dfcomments.csv")
    keywords = ["password", "token", "key", "pwd", "secret", "encrypt",
                "debug", "internal", "confidential", "cookie", "admin", "jwt"]
    hits = df_comments[
        df_comments["Comment"].str.contains(
            "|".join(keywords), flags=re.IGNORECASE, na=False
        )
    ]
    print(f"  Found {len(hits)} comments matching keywords:")
    print(hits.to_string(index=False))

    print("\n[DONE] All output files saved: dftitles.csv, dflinks.csv, dfcomments.csv")


if __name__ == "__main__":
    main()
