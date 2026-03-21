#!/usr/bin/env python
# coding: utf-8

# ## Project Straylight
# 
# ### Common Crawl Domain Reconnaissance
# 
# __Introduction:__
# This notebook will provide the ability to configure and search the publicly available Common Crawl dataset of websites. Common Crawl is a freely available dataset which contains over 8 years of crawled data including over 25 billion websites, trillions of links, and petabytes of data.
# 
# __GitHub:__
# * https://github.com/brevityinmotion/straylight
# 
# __Blog:__
# * [Search the html across 25 billion websites for passive reconnaissance using common crawl](https://medium.com/@brevityinmotion/search-the-html-across-25-billion-websites-for-passive-reconnaissance-using-common-crawl-7fe109250b83?sk=5b8b4a7c506d5acba572c0b30137f7aa)
# 
# ___Credits:___
# * Special thank you to Sebastian Nagel for the tutorials and insight for utilizing the dataset!
# * Many of the functions and code have been adapted from: http://netpreserve.org/ga2019/wp-content/uploads/2019/07/IIPCWAC2019-SEBASTIAN_NAGEL-Accessing_WARC_files_via_SQL-poster.pdf
# 

# ### Prior to utilizing this notebook, the following three queries should be run within AWS Athena to configure the Common Crawl database.
# 
# #### Query 1
# <code>CREATE DATABASE ccindex</code>
# 
# #### Query 2
# <code>CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (
#   url_surtkey                   STRING,
#   url                           STRING,
#   url_host_name                 STRING,
#   url_host_tld                  STRING,
#   url_host_2nd_last_part        STRING,
#   url_host_3rd_last_part        STRING,
#   url_host_4th_last_part        STRING,
#   url_host_5th_last_part        STRING,
#   url_host_registry_suffix      STRING,
#   url_host_registered_domain    STRING,
#   url_host_private_suffix       STRING,
#   url_host_private_domain       STRING,
#   url_protocol                  STRING,
#   url_port                      INT,
#   url_path                      STRING,
#   url_query                     STRING,
#   fetch_time                    TIMESTAMP,
#   fetch_status                  SMALLINT,
#   content_digest                STRING,
#   content_mime_type             STRING,
#   content_mime_detected         STRING,
#   content_charset               STRING,
#   content_languages             STRING,
#   warc_filename                 STRING,
#   warc_record_offset            INT,
#   warc_record_length            INT,
#   warc_segment                  STRING)
# PARTITIONED BY (
#   crawl                         STRING,
#   subset                        STRING)
# STORED AS parquet
# LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';</code>
# 
# #### Query 3
# <code>MSCK REPAIR TABLE ccindex</code>

# In[ ]:


import json
import boto3
import os


# In[ ]:


# Run the core configurations notebook. This generally only needs to be run once.
get_ipython().run_line_magic('run', './configuration.ipynb')


# In[ ]:


# The core functions notebook contains generalized functions that apply across use cases
get_ipython().run_line_magic('run', './corefunctions.ipynb')


# In[ ]:


# Install additional dependencies for common crawl from the configuration.ipynb notebook
dependencies_commoncrawl()


# In[ ]:


# Make sure to update these values
DOMAIN_TO_QUERY = 'derbycon.com' # This should look like 'domain.com'. The wildcard will be added automatically later.
ATHENA_BUCKET = 's3://brevity-athena' # This will need to be customized and specific to your own account (i.e. s3://customname-athena').
ATHENA_DB = 'ccindex' # This should align with the database and not need changed if it was created using the previous queries.
ATHENA_TABLE = 'ccindex' # This should align with the table and not need changed if it was created using the previous queries.

# Do not modify this query unless the intent is to customize
query = "SELECT url, url_query, warc_filename, warc_record_offset, warc_record_length, fetch_time FROM %s WHERE subset = 'warc' AND url_host_registered_domain = '%s';" % (ATHENA_TABLE, DOMAIN_TO_QUERY)

get_ipython().run_line_magic('time', 'execid = queryathena(ATHENA_DB, ATHENA_BUCKET, query)')
print(execid)


# In[ ]:


import json, boto3, time, requests
import pandas as pd
import io

# Load an external notebook with normalized functions
get_ipython().run_line_magic('run', './corefunctions.ipynb')

# Utilize executionID to retrieve results
downloadURL = retrieveresults(execid)

# Load output into dataframe
s=requests.get(downloadURL).content
dfhosts=pd.read_csv(io.StringIO(s.decode('utf-8')))
dfhosts


# In[ ]:


# Drop duplicates keeping the latest version - if you want to review changes between fetch, you may not want to run this
dfhosts = dfhosts.sort_values('fetch_time').drop_duplicates('url',keep='last',ignore_index=True)
dfhosts


# In[ ]:


# Output results to excel spreadsheet
dfhosts['url'].to_excel("cc-urls.xlsx") 


# In[ ]:


pd.set_option('display.max_colwidth', None)
dfhosts['url'].head(10)


# In[ ]:


import warcio
from warcio.archiveiterator import ArchiveIterator
import os
from bs4 import BeautifulSoup 
from bs4 import Comment
import io
from io import BytesIO
import logging
import dask.dataframe as ddf
import multiprocessing
import asyncio

# Thank you to Sebastian Nagel for your instructions and code to perform the following step.
# http://netpreserve.org/ga2019/wp-content/uploads/2019/07/IIPCWAC2019-SEBASTIAN_NAGEL-Accessing_WARC_files_via_SQL-poster.pdf
titles_list = []
uris_list = []
links_list = []
comments_list = []

#Fetch all WARC records defined by filenames and offsets in rows, parse the records and the contained HTML, split the text into words and emit pairs <word, 1>
def processwarcrecords(dfhosts, writefiles, searchfiles, howmanyrecords):
    s3client = boto3.client('s3')
    recordcount = 0
    skippedrecords = 0
    processedrecords = 0
    totalrecords = len(dfhosts.index)
    if howmanyrecords == 0:
        howmanyrecords = totalrecords
    async def analyzeDFRows(index, row, semaphore):
    # for index, row in dfhosts.iterrows():
        async with semaphore:
            if recordcount > howmanyrecords:
                break
            recordcount = recordcount + 1
            print('Processing row ' + str(recordcount) + 
                  ' of ' + str(totalrecords) + ' total rows.')
            print('Processed ' + str(processedrecords) + ' records.')
            url = row['url']
            warc_path = row['warc_filename']
            offset = int(row['warc_record_offset'])
            length = int(row['warc_record_length'])
            rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
            response = s3client.get_object(Bucket='commoncrawl',
                                           Key=warc_path,Range=rangereq)
            record_stream = BytesIO(response["Body"].read())
            for record in ArchiveIterator(record_stream):
                if record.rec_type == 'response':
                    try:
                        warc_target_uri = record.rec_headers.get_header('WARC-Target-URI')

                        page = record.content_stream().read()
                        # lxml should be faster but is not
                        soup = BeautifulSoup(page, 'html.parser') 
                        title = soup.title.string
                        titles_list.append((warc_target_uri, title))
                        uris_list.append((warc_target_uri))
                        if searchfiles == 'yes':
                            # Find all links
                            for link in soup.find_all('a'):
                                links_list.append((warc_target_uri, link.get('href')))
                            # Find all links
                            for comment in soup.find_all(text=lambda text: isinstance(text, Comment)):
                                comments_list.append((warc_target_uri, comment))
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

        sem = asyncio.Semaphore(value=3)

        tasks = [analyzeDFRows(index, row, sem) for (index, row) in dfhosts.iterrows()]
        await asyncio.gather(*tasks)

    asyncio.run(main())

searchfiles = 'yes' # anything other than 'yes' will not process
writefiles = 'no' # anything other than 'yes' will not process
howmanyrecords = 0 # 0 is all records; other options would be a numeric value
processwarcrecords(dfhosts,writefiles,searchfiles,howmanyrecords)

#df_dask = ddf.from_pandas(dfhosts, npartitions=4)   # where the number of partitions is the number of cores you want to use
#df_dask.apply(lambda x: processwarcrecords(dfhosts,writefiles,searchfiles,howmanyrecords), axis=1, meta=('str')).compute(scheduler='multiprocessing')
#df_dask['output'] = df_dask.apply(lambda x: your_function(x), meta=('str')).compute(scheduler='multiprocessing')



# In[ ]:


# Load lists into dataframes for further processing
dfcomments = pd.DataFrame(comments_list,columns=['URI','Comment'])
dftitles = pd.DataFrame(titles_list,columns=['URI','Title'])
dflinks = pd.DataFrame(links_list,columns=['URI','Link'])
#dflinks.head(10)
dfcomments.head(10)


# In[ ]:


# Search for keywords within the comments
import re
pd.set_option('display.max_colwidth', None)
search_values = ['password','token','key','pwd','secret','encrypt','debug','internal','confidential','cookie','admin','jwt']
dfcomments[dfcomments.Comment.str.contains('|'.join(search_values ),flags=re.IGNORECASE)]


# In[ ]:


# Export html search results to excel

#with pd.ExcelWriter('cc-domains.xlsx') as writer:  
#    dfcomments.to_excel(writer, sheet_name='comments')
#    dftitles.to_excel(writer, sheet_name='titles')
#    dflinks.to_excel(writer, sheet_name='links')

# if dataframe has over 65535 rows, Excel will skip data. In this situation, .csv is better.
#compression_opts = dict(method='zip',archive_name='out.csv')  
dfcomments.to_csv('dfcomments.csv', header=True, index=False)
dftitles.to_csv('dftitles.csv', header=True, index=False) 
dflinks.to_csv('dflinks.csv', header=True, index=False) 


# In[ ]:


# Zip the file for download out of Jupyter
filepath = os.getcwd() + '/tmp/'
# This will create a file named domainoutput.tar.gz with the full html files in the structure of the website. It can be downloaded from the same directory running the notebook.
get_ipython().system(' tar -zcvf domainoutput.tar.gz $filepath')
# This will clean-up the tmp folder
get_ipython().system(' rm -rf tmp/*')


# In[ ]:


# Additional example queries to run with this configuration.
# These can be run directly within the Athena query window in the AWS console or can be integrated into this notebook instead of using the pre-defined query.

# Search the entire common crawl data set for specific URL parameters.
SELECT url,
       warc_filename,
       warc_record_offset,
       warc_record_length,
       url_query
FROM "ccindex"."ccindex"
WHERE subset = 'warc'
  AND url_query like 'cmd='


# In[ ]:


# Count the number of distinct websites for a specific domain

SELECT DISTINCT COUNT(url) as URLCount
FROM "ccindex"."ccindex"
WHERE  subset = 'warc'
  AND url_host_registered_domain = 'domain.com'

