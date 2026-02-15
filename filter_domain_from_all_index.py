# coding: utf-8
import sys
allindexfile = sys.argv[1]
outputfile = sys.argv[2]
domain = sys.argv[3]
s=""
with open(allindexfile) as inf:
    with open(outputfile, 'w') as ouf:
        lines = inf.readlines()
        for line in lines:
            #line = eval(line)
            if domain in line:
                #print(line)
                s += line.strip()
                s += ','
            #if len(s) > 1000: 
            #    break
        s = s.rstrip(',')
        s = '[' + s + ']'
        ouf.write(s)
        
