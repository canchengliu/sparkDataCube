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
batch_dict['2 3 4 5 6 7'] = 6
batch_dict['2 3 5 6 7'] = 3
batch_dict['2 5 6 7'] = 3
batch_dict['5 6 7'] = 3

# -- get C --
def seq(start, end):
	return [range(start, i) for i in range(start, end + 2)]

def getC():
	return [a + b for a, b in product(seq(2, 4), seq(5, 7))]
# -- get C --

li = ['1', '2', '3', '4']

def comb(li):
	coms = []
	str_coms = []
	sort_coms = []
	for n in range(1, len(li) + 1):
		coms.extend(itertools.combinations(li, n))
	for c in coms:
		str_coms.append(' '.join(list(c)))
	sort_str_com = sorted(str_coms)
	for s in sort_str_com:
		sort_coms.append(s.split())
	print sort_coms



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
		record = ' '.join([str(i) for i in R[0]])+'|'+' '.join(k)
		batch_len = len(R)
		uid = data[1]
		value = "%s\t%s" % (record, uid)
		yield (str(batch_id), value)
		batch_id += 1
# -- batch_mapper --

"""


lgorithm 2. Overall MR-Cube Algorithm MR-CUBE(Cube Lattice C, Data set D, Measure M)
1 Dsample 1⁄4 SAMPLEðDÞ
2 RegionSizes R 1⁄4 ESTIMATE-MAPREDUCE
ðDsample ; CÞ
3 Ca 1⁄4 ANNOTATEðR; CÞ # value part. & batching 4 while (D)
5 do R
6 D
7 Ca
8 Result
9 return Result
R [ MR-CUBE-MAPREDUCEðCa ; M; DÞ
D’ # retry failed groups D’ from MR-Cube-Reduce INCREASE-PARTITIONINGðCa Þ
MERGE(R)# post-aggregate value partitions
Algorithm 3. MR-Cube Phase 1: Annotation MapReduce ESTIMATE-Map(e)
1 #eisatupleinthedata
2 let C be the Cube Lattice;
3 foreachci inC
4 do EMITðci;ciðeÞ ) 1Þ # the group is the secondary key
ESTIMATE-REDUCE/COMBINE(hr; gi; fe1; e2; . . .g) 1 # hr; gi are the primary/secondary keys
2 MaxSizeS fg
3 for each r,g
4 do S[r] MAX(S[r],jgj)
5 # jgj is the number of tuples fei;...;ejg 2 g 6 return S

Algorithm PipeSort

function PS_MAP(e):
	# e is a tuple in the dataset
	C = getPipeline() // C is a list of pipeline, pipeline is a list of group with the same 前缀
	batch_id = 0
	for all R in C do:
		re = R(e) // R(e): extra the 属性 of longest group in R from e
		do EMIT(batch_id, re=>uid) // uid: record id, the uid of tuple e
		batch_id += 1

function PS_REDUCE(batch_id, [kv1, kv2, kv3....]):
	


"""

# -- top_down --
def top_down(data):
	global batch_dict
	uidset = {}
	lastkey = ['0','0','0','0','0','0','0','0','0']

	sort_data = sorted(data)
	batch_len = batch_dict[sort_data[0].split('|')[0]]

	for line in sort_data: # line : {region}|{group}\t{uid}
		record, uid = line.split('\t')
		region, group = record.split('|')
		region_list = region.split()
		group_list = group.split()
		region_len = len(region_list)

		for cur_len in range(batch_len, 0, -1): # [batch_len, 0) reverse
			s = ' '.join(region_list[0:region_len]) + '|' + ' '.join(group_list[0:region_len])
			if s not in uidset:
				uidset[s] = set()
			uidset[s].add(uid)
			if lastkey[cur_len] == '0':
				lastkey[cur_len] = s
			if lastkey[cur_len] != s:
				uidstr = ' '.join(uidset[lastkey[cur_len]])
				yield "%s\t%s" % (lastkey[cur_len], uidstr)
				uidset.pop(lastkey[cur_len])
				lastkey[cur_len] = s
			region_len -= 1

	for i in range(batch_len, 0, -1):
		uidstr = ' '.join(uidset[lastkey[i]])
		yield "%s\t%s" % (lastkey[i], uidstr)
# -- top_down --



# -- navie_reducer --
def navie_reducer(data):
	pass
# -- navie_reducer --

def main_func(sc, rdd):
	cnt1 = rdd.count()
	cnt2 = rdd.flatMap(navie_mapper).count()
	print "&&&&&&&&%d&&&&&&%d&&&&&&&&&&" % (cnt1, cnt2)
	#Sometimes we need a sample of our data in our driver program. The takeSample(withReplacement, num, seed) function allows us to take a sample of our data either with or without replacement.
	# Keep in mind that your entire dataset must fit in memory on a single machine to use collect() on it, so collect() shouldn’t be used on large datasets.
	# tmp = rdd.flatMap(navie_mapper).groupByKey().collect()
	# visits = map((lambda (x,y): (x, set(y))), tmp)
	
	#tmp = rdd.flatMap(batch_mapper).collect()

	tmp = rdd.flatMap(batch_mapper).groupByKey().flatMapValues(top_down).collect()
	#tmp = rdd.flatMap(batch_mapper).groupByKey().flatMapValues(top_down).collect()
	print tmp

# -- navie_reducer --
if __name__ == "__main__":
	print "#####################################"
	sc = SparkContext(appName="Co-occurrence")
	rdd = sc.textFile("file:///Users/liucancheng/Documents/GitHub/sparkDataCube/test.txt")
	main_func(sc, rdd)
	print "888888888888888888888888888888888888888888888888888888888888888888888888888888888888888"







