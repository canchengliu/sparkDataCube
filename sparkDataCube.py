#!/usr/bin/env python
# -*- coding: utf-8 -*- 
# -- Referenct --
#Does groupByKey in Spark preserve the original order? http://stackoverflow.com/questions/24206660/does-groupbykey-in-spark-preserve-the-original-order
#Spark sort by key and then group by to get ordered iterable?  http://stackoverflow.com/questions/29792320/spark-sort-by-key-and-then-group-by-to-get-ordered-iterable
#
# -- Referenct --
#sample(withReplacement, fraction, seed=None) histogram(buckets) may be used for TSCube

import sys
import os
import random
import math
from itertools import product
from pyspark import SparkContext


batch_dict = {}
batch_dict['234567'] = 6
batch_dict['23567'] = 3
batch_dict['2567'] = 3
batch_dict['567'] = 3

# -- get C --
def seq(start, end):
	return [range(start, i) for i in range(start, end + 2)]

def getC():
	return [a + b for a, b in product(seq(2, 4), seq(5, 7))]
# -- get C --

# -- navie_mapper --
def navie_mapper(line): # line: line record
	e = line.split()
	C = getC()
	for R in C:
		k = [e[i] for i in R]
		#yield "%s|%s\t%s" % (' '.join([str(i) for i in R]), ' '.join(k), e[1])
		key = "%s|%s" % (' '.join([str(i) for i in R]), ' '.join(k))
		value = e[1]
		yield (key, value)	
	
# -- navie_mapper --

# -- batch_mapper --
def batch_mapper(line):
	data = line.strip().split()
	C = [[[2, 3, 4, 5, 6, 7], [2, 3, 4, 5, 6], [2, 3, 4, 5], [2, 3, 4], [2, 3], [2]], [[2, 3, 5, 6, 7], [2, 3, 5, 6], [2, 3, 5]], [[2, 5, 6, 7], [2, 5, 6], [2, 5]], [[5, 6, 7], [5, 6], [5]]]
	#C = getC()
	batch_id = 0 # can set a batch_id:batch_len dictionary to broadcast
	for R in C:
		k = [data[i] for i in R[0]]
		key = ' '.join([str(i) for i in R[0]])+'|'+' '.join(k)
		batch_len = len(R)
		uid = data[1]
		value = "%s\t%s" % (key, uid)
		yield (str(batch_id), value)
		batch_id += 1
# -- batch_mapper --


# -- top_down --
def top_down(data):
	global batch_dict
	sort_data = sorted(data)
	batch_len = batch_dict[sort_data[0].split('|')[0]]
	
	for line in sort_data:
		




		s = line.split('\t')
		rvalue = s[0] #|parition|.|record_segment|
		uid = s[1].split() #[|ID|, |batch_len|] 
		ids = uid[0] #|ID|
		batch_len = int(uid[1]) #int(|batch_len|)
		if lastbatch_len == 0:
			lastbatch_len = batch_len
		if lastbatch_len != batch_len: #batch_len changed and not the first => flag = 1
			flag = 1
			lastbatch_len = batch_len
		rvalue = rvalue.split('|') # [|parition|.|region|, |group|]
		temp = rvalue[0].split('.') # [|parition|, |region|]
		region = temp[1].split() # reigon_list
		group  = rvalue[1].split() # group_list
		r_len = len(region) # region_len
		while batch_len != 0:
			s = ' '.join(region[0:r_len]) + '|' + ' '.join(group[0:r_len])
			if uidset.get(s,'none') == 'none':
				uidset[s] = set()
			uidset[s].add(ids) # group s add an id_list
			if lastkey[batch_len] == 0: # the first record of different region within the batch
				lastkey[batch_len] = s
			if lastkey[batch_len] != s: # group change
				uidstr = ' '.join(uidset[lastkey[batch_len]])
				if flag == 0: # the same batch
					print "%s\t%s" % (lastkey[batch_len], uidstr)
				else:
					pass
				uidset.pop(lastkey[batch_len])
				lastkey[batch_len] = s
			batch_len = batch_len - 1
			r_len = r_len - 1
		flag = 0
	for i in range(1,lastbatch_len+1):
		uidstr = ' '.join(uidset[lastkey[i]])
		print "%s\t%s" % (lastkey[i], uidstr)


	
# -- top_down --



# -- navie_reducer --
def navie_reducer(data):
	pass

def main_func(sc, rdd):
	cnt1 = rdd.count()
	cnt2 = rdd.flatMap(navie_mapper).count()
	print "&&&&&&&&%d&&&&&&%d&&&&&&&&&&" % (cnt1, cnt2)
	#Sometimes we need a sample of our data in our driver program. The takeSample(withReplacement, num, seed) function allows us to take a sample of our data either with or without replacement.
	# Keep in mind that your entire dataset must fit in memory on a single machine to use collect() on it, so collect() shouldn’t be used on large datasets.
	# tmp = rdd.flatMap(navie_mapper).groupByKey().collect()
	# visits = map((lambda (x,y): (x, set(y))), tmp)
	
	#tmp = rdd.flatMap(batch_mapper).collect()

	rdd.flatMap(batch_mapper).groupByKey().flatMapValues(top_down).collect()


	print tmp

# -- navie_reducer --
if __name__ == "__main__":
	print "#####################################"
	sc = SparkContext(appName="Co-occurrence")
	rdd = sc.textFile("file:///Users/liucancheng/Documents/GitHub/sparkDataCube/test.txt")
	main_func(sc, rdd)
	print "888888888888888888888888888888888888888888888888888888888888888888888888888888888888888"






