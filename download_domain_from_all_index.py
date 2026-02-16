# coding: utf-8
"""
this program reads common crawl all index file;  loops through it; parases it; download the warc file; 
m, n helps pick up a where to start reading the input index
"""
import os
import time
m = 0
s=""
with open('underarmour_all_index.txt') as inf:
    with open('filtered_underarmour_all_index.txt', 'w') as ouf:
        #lines = inf.readline()
        for line in inf:
            m += 1
            if m>=101:
                #line = eval(line)
                if "www.underarmour.com" in line:
                    print(line)
                    #s += line.strip()
                    #s += ','
                    line = eval(line)
                    print(type(line))
                    filename = line['filename']
                    offset = line['offset']
                    length = line['length']
                    status = line['status']
                    print(filename, offset, length)
                if m == 200:
                    break
                if status == '418':
                    continue
            
                start = int(offset)
                end = start + int(length) - 1
                baseurl = "https://data.commoncrawl.org/"
                (_, warcfile) = os.path.split(filename)
                outputdir = "warcdir"
                downloadurl = f'curl -H \"Range: bytes={start}-{end}\" \"{baseurl + filename}\" --output \"{outputdir}/{warcfile}\"'
                os.system(downloadurl)
                time.sleep(3)
