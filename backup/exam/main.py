# -*- coding: UTF-8 -*- 
import sys


def f(line):
	#global shared_dicts
	ip = line.split(" ")[0]#.encode('ascii', 'ignore')
	url = line.split(" ")[6]
	return shared_dict.value.items()[0]
	#if url in shared_dict:
	#	index = shared_dict[url]
	#	return (ip, index)

def main_func(sc, rdd, url_dict):
	if __name__ == "__main__":
		# Only run this on the driver node
	global shared_dict
	shared_dict = sc.broadcast(url_dict)
	visits = rdd.map(f).collect()
	for v in visits:
		print v

"""
def f2(li):
	return (li[0], len(set(li[1])))

def main(rdd):
	li = rdd.map(f).groupByKey().map(f2).collect()#reduceByKey(lambda a, b : a + b).map(lambda (k, v) : (v, k)).sortByKey().top(20) #li += rdd.map(f).reduceByKey(lambda a, b : a + b).map(lambda (k, v) : (v, k)).sortByKey().top(10)
	joinli = sorted(li, key = lambda r : r[1], reverse=True)
	for r in joinli[:20]:
		print r[0] + '\t' + str(r[1])
"""

def hash(value):
	h = value
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h