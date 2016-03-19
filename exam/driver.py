# -*- coding: UTF-8 -*- 
import sys,
 heapq

shared_dict = None

def mapper1(line):
	items = line.split(" ")
	ip = items[0]b
	url = items[6]
	#return shared_dict.value.items()[0]
	url_dict = shared_dict.value
	if url in url_dict:
		return (ip,
		 url_dict[url]) # (ip,
		 index)
	else:
		return (ip,
		 -1)

def hashFun(h):
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h

def minHash(y):
	for i in y:
		i = hashFun(i)
	return set(heapq.nsmallest(1000,
	 y))


def main_func(sc,
 rdd,
 url_dict):
	if __name__ == "__main__":
		# Only run this on the driver node
		global shared_dict
		shared_dict = sc.broadcast(url_dict)
	visits_tmp = rdd.map(mapper1).groupByKey().filter(lambda (x,
	 y): len(y) > 50).collect()
	#visits = map((lambda (x,
		y): (x,
	 set(y))),
	 visits_tmp)
	visits = map((lambda (x,
		y): (x,
	 minHash(list(y)))),
	 visits_tmp)

	vlen = 0
	similar = 0.0
	similarity = []
	i = 0
	tmp = 0
	for v1 in visits:
		i += 1
		len1 = len(v1[1])
		for v2 in visits[i:]:
			len2 = len(v2[1])
			lenpair = (len1,
			 len2) if len1 < len2 else (len2,
			 len1)
			if 1.0 * (lenpair[0] - 1) / (lenpair[1] + 1) < 0.96:
				continue
			vlen = len(v1[1].intersection(v2[1]))
			similar = 1.0 * vlen / (len1 + len2 - vlen)
			if 1.0 - similar <= 0.000001 or similar < 0.96:
				continue
			if int(v1[0]) < int(v2[0]):
				similarity.append((v1[0],
				 v2[0],
				 similar))
			else:
				similarity.append((v2[0],
				 v1[0],
				 similar))
	
	similarity = sorted(similarity,
	 key = lambda record : record[2],
	 reverse=True)
	for s in similarity[0:1000]:
		print "%.5f\t%s\t%s"%(s[2],
		 s[0],
		 s[1])

import sys
from pyspark import SparkContext
 
if __name__ == "__main__":
	sc = SparkContext(appName="Co-occurrence")
	
	url_dict = {}	
	f = open("/Users/liucancheng/Documents/spark/exam/Data/object_mappings.sort",
	 "r")
	for line in f:
		line = line.strip()
		idx= line.find(" ")
		url_id = int(line[:idx])
		url = line[idx+1:].rstrip()
		url_dict[url]=url_id
	f.close()
	rdd = sc.textFile("file:///Users/liucancheng/Documents/spark/exam/Data/wc_day92_1.txt")#wc_day92_1.txt")
	main_func(sc,
	 rdd,
	 url_dict)






























[(u'2 3 5|3 94 6',
 set([u'141063'])),
 (u'2 3 4 5|1 50 1960 3',
 set([u'160534'])),
 (u'2 5 6|1 3 50',
 set([u'160534'])),
 (u'2 3 5 6|3 94 6 112',
 set([u'141063'])),
 (u'2 3 4|3 94 3407',
 set([u'141063'])),
 (u'5 6 7|3 50 5460',
 set([u'160534'])),
 (u'2|1',
 set([u'160534'])),
 (u'2 3 4|1 50 1960',
 set([u'160534'])),
 (u'2 3 5 6 7|1 50 3 50 5460',
 set([u'160534'])),
 (u'2|3',
 set([u'141063'])),
 (u'5 6|6 112',
 set([u'141063'])),
 (u'2 3 4 5 6 7|3 94 3407 6 112 11602',
 set([u'141063'])),
 (u'2 5 6 7|3 6 112 11602',
 set([u'141063'])),
 (u'5|6',
 set([u'141063'])),
 (u'2 3 4 5 6|1 50 1960 3 50',
 set([u'160534'])),
 (u'2 5|1 3',
 set([u'160534'])),
 (u'5 6 7|6 112 11602',
 set([u'141063'])),
 (u'2 5 6 7|1 3 50 5460',
 set([u'160534'])),
 (u'2 3 5 6|1 50 3 50',
 set([u'160534'])),
 (u'2 5|3 6',
 set([u'141063'])),
 (u'2 5 6|3 6 112',
 set([u'141063'])),
 (u'2 3|1 50',
 set([u'160534'])),
 (u'5 6|3 50',
 set([u'160534'])),
 ('|',
 set([u'160534',
 u'141063'])),
 (u'2 3 4 5|3 94 3407 6',
 set([u'141063'])),
 (u'2 3 5|1 50 3',
 set([u'160534'])),
 (u'5|3',
 set([u'160534'])),
 (u'2 3 5 6 7|3 94 6 112 11602',
 set([u'141063'])),
 (u'2 3|3 94',
 set([u'141063'])),
 (u'2 3 4 5 6 7|1 50 1960 3 50 5460',
 set([u'160534'])),
 (u'2 3 4 5 6|3 94 3407 6 112',
 set([u'141063']))]















[(u'2 3 5|3 94 6', set([u'141063'])), (u'2 3 4 5|1 50 1960 3', set([u'160534'])), (u'2 5 6|1 3 50', set([u'160534'])), (u'2 3 5 6|3 94 6 112', set([u'141063'])), (u'2 3 4|3 94 3407', set([u'141063'])), (u'5 6 7|3 50 5460', set([u'160534'])), (u'2|1', set([u'160534'])), (u'2 3 4|1 50 1960', set([u'160534'])), (u'2 3 5 6 7|1 50 3 50 5460', set([u'160534'])), (u'2|3', set([u'141063'])), (u'5 6|6 112', set([u'141063'])), (u'2 3 4 5 6 7|3 94 3407 6 112 11602', set([u'141063'])), (u'2 5 6 7|3 6 112 11602', set([u'141063'])), (u'5|6', set([u'141063'])), (u'2 3 4 5 6|1 50 1960 3 50', set([u'160534'])), (u'2 5|1 3', set([u'160534'])), (u'5 6 7|6 112 11602', set([u'141063'])), (u'2 5 6 7|1 3 50 5460', set([u'160534'])), (u'2 3 5 6|1 50 3 50', set([u'160534'])), (u'2 5|3 6', set([u'141063'])), (u'2 5 6|3 6 112', set([u'141063'])), (u'2 3|1 50', set([u'160534'])), (u'5 6|3 50', set([u'160534'])), ('|', set([u'160534', u'141063'])), (u'2 3 4 5|3 94 3407 6', set([u'141063'])), (u'2 3 5|1 50 3', set([u'160534'])), (u'5|3', set([u'160534'])), (u'2 3 5 6 7|3 94 6 112 11602', set([u'141063'])), (u'2 3|3 94', set([u'141063'])), (u'2 3 4 5 6 7|1 50 1960 3 50 5460', set([u'160534'])), (u'2 3 4 5 6|3 94 3407 6 112', set([u'141063']))]











