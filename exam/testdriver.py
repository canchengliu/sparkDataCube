# -*- coding: UTF-8 -*- 
import sys, heapq

shared_dict = None

def mapper1(line):
	items = line.split(" ")
	ip = items[0].encode('ascii', 'ignore')
	url = items[6]
	#return shared_dict.value.items()[0]
	url_dict = shared_dict.value
	if url in url_dict:
		return (ip, url_dict[url]) # (ip, index)
	else:
		return (ip, -1)

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
	return set(heapq.nsmallest(1000, y))


def main_func(sc, rdd, url_dict):
	if __name__ == "__main__":
		# Only run this on the driver node
		global shared_dict
		shared_dict = sc.broadcast(url_dict)
	visits_tmp = rdd.map(mapper1).groupByKey().filter(lambda (x, y): len(y) > 10).collect()
	visits = map((lambda (x,y): (x, set(y))), visits_tmp)

	vlen = 0
	similar = 0.0
	similarity = []
	i = 0
	"""
	585131 666261
	0.99000 55236 368225
	0.98990 75385 586417
	0.98990 91352 634896
	0.98990 586451 635967
	"""
	for v1 in visits:
		i += 1
		len1 = len(v1[1])
		for v2 in visits[i:]:
			len2 = len(v2[1])
			vpair = (v1[0], v2[0]) if v1[0] < v2[0] else (v2[0], v1[0])
			if (vpair[0] == 585131 and vpair[1] == 666261) or (vpair[0] == 55236 and vpair[1] == 368225) or (vpair[0] == 75385 and vpair[1] == 586417):
				vlen = len(v1[1].intersection(v2[1]))
				similar = 1.0 * vlen / (len1 + len2 - vlen)
				similarity.append((v1[0], v2[0], similar))
	
	for s in similarity:
		print "%.5f\t%s\t%s"%(s[2], s[0], s[1])

import sys
from pyspark import SparkContext
 
if __name__ == "__main__":
	sc = SparkContext(appName="Co-occurrence")
	
	url_dict = {}	
	f = open("/Users/liucancheng/Documents/spark/exam/Data/object_mappings.sort", "r")
	for line in f:
		line = line.strip()
		idx= line.find(" ")
		url_id = int(line[:idx])
		url = line[idx+1:].rstrip()
		url_dict[url]=url_id
	f.close()
	rdd = sc.textFile("file:///Users/liucancheng/Documents/spark/exam/Data/wc_day92_1.txt")#wc_day92_1.txt")
	main_func(sc, rdd, url_dict)


