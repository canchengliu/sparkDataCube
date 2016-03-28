#!/usr/bin/env python
# -*- coding: utf-8 -*- 
# -- Referenct --
#Does groupByKey in Spark preserve the original order? http://stackoverflow.com/questions/24206660/does-groupbykey-in-spark-preserve-the-original-order
#Spark sort by key and then group by to get ordered iterable?  http://stackoverflow.com/questions/29792320/spark-sort-by-key-and-then-group-by-to-get-ordered-iterable
#
# -- Referenct --
#sample(withReplacement, fraction, seed=None) histogram(buckets) may be used for TSCube

# how spark run http://www.cnblogs.com/shishanyuan/archive/2015/08/19/4721326.html
import os
import random
import math
from itertools import product
from itertools import combinations
import sys
import getopt
from pyspark import SparkContext
from insert_db import insert_db

# -- get C --
def seq(start, end):
	return [range(start, i) for i in range(start, end + 2)]

def getC():
	return [a + b for a, b in product(seq(2, 3), seq(4, 5))]

def all_combinations(li):
	res = []
	for size in range(len(li) + 1):
		for c in combinations(li, size):
			res.append(list(c))		
	return res


# -- get C 2--
def get_all_regions(hierarchy_col, common_col):
	if len(hierarchy_col) == 0:
		return all_combinations(common_col)
	pre_tup = seq(hierarchy_col[0][0], hierarchy_col[0][1])
	for tup in hierarchy_col[1:]:
		pre_tup = [a + b for a, b in product(pre_tup, seq(tup[0], tup[1]))]
	if len(common_col) == 0:
		return pre_tup
	common_list =  all_combinations(common_col)# all common column combinations
	res = [a + b for a, b in product(pre_tup, common_list)]
	res.sort()
	return res

def get_C(hierarchy_col, common_col):
	region = get_all_regions(hierarchy_col, common_col)
	m = 0
	mb = 0
	if len(common_col) != 0:
		m = max(common_col) # column
	if len(hierarchy_col) != 0:
		mb = max([max(hc) for hc in hierarchy_col])
	if mb > m:
		m = mb
	l = len(region) # row
	full_region = [[-1] * (m + 1) for t in range(l)] # set every cell to min, means *
	for i in range(l):
		for r in region[i]:
			full_region[i][r] = r
	full_region.sort()
	sort_region = []
	for fr in full_region:
		tmp = []
		for f in fr:
			if f != -1:
				tmp.append(f)
		sort_region.append(tmp)
		tmp = []
	sort_region.reverse()
	#print sort_region
	batch = []
	pre = sort_region[0]
	tmp = [pre]
	for sr in sort_region[1:]:
		if len(sr) < len(pre) and pre[:len(sr)] == sr: #same prefix
			tmp.append(sr)
		else:
			batch.append(tmp)
			tmp = [sr]
		pre = sr
	batch.append(tmp)
	print batch
	return batch

def get_batch_len(batch):
	bl = [b[0] for b in batch]
	res = {}
	k = 0
	for b in bl:
		s = ' '.join([str(i) for i in b])
		res[s] = len(batch[k])
	return res

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
	#C = [[[2, 3, 4, 5, 6, 7], [2, 3, 4, 5, 6], [2, 3, 4, 5], [2, 3, 4], [2, 3], [2]], [[2, 3, 5, 6, 7], [2, 3, 5, 6], [2, 3, 5]], [[2, 5, 6, 7], [2, 5, 6], [2, 5]], [[5, 6, 7], [5, 6], [5]]]
	#C = getC_2(hierarchy_col, common_col)
	batch_id = 0 # can set a batch_id:batch_len dictionary to broadcast
	for R in shared_batch.value:
		k = [data[i] for i in R[0]]
		record = ' '.join([str(i) for i in R[0]])+'|'+' '.join(k)
		batch_len = len(R)
		uid = data[shared_id_col.value]
		value_col = [data[vc] for vc in shared_value_col.value]
		value = "%s\t%s_%s" % (record, uid, ' '.join(value_col))
		yield (str(batch_id), value)
		batch_id += 1
# -- batch_mapper --



#broadcast ID, value_list, batch_list(batch_id)

# -- top_down --
def top_down(data):
	sort_data = sorted(data)
	#batch_len = batch_dict[]

	batch_len = shared_batch_dict.value[sort_data[0].split('|')[0]]
	uidset = {}
	valueset = {} # sum(value) 
	lastkey = ['0'] * (batch_len + 1)
	for line in sort_data: # line : {region}|{group}\t{uid}|{value}
		record, uid_value = line.split('\t')
		region, group = record.split('|')
		region_list = region.split()
		group_list = group.split()
		region_len = len(region_list)
		uid, value = uid_value.split('_')


		for cur_len in range(batch_len, 0, -1): # [batch_len, 0) reverse
			s = ' '.join(region_list[0:region_len]) + '|' + ' '.join(group_list[0:region_len])
			if s not in uidset:
				uidset[s] = set()
				valueset[s] = set()
			uidset[s].add(uid)
			valueset[s].add(value)
			if lastkey[cur_len] == '0':
				lastkey[cur_len] = s
			if lastkey[cur_len] != s:
				#uidstr = ' '.join(uidset[lastkey[cur_len]])
				#valuestr = ' '.join(valueset[lastkey[cur_len]])
				#yield "%s\t%s|%s" % (lastkey[cur_len], uidstr, valuestr)
				vv = [[float(v) for v in vs.split()] for vs in valueset[lastkey[cur_len]]]
				vv_t = map(list, zip(*vv)) # transform
				v_sum = [str(sum(vt)) for vt in vv_t]
				valuestr = ' '.join(v_sum)
				yield "%s\t%d %s" % (lastkey[cur_len], len(uidset[lastkey[cur_len]]), valuestr)
				post = "%s\t%d %s" % (lastkey[cur_len], len(uidset[lastkey[cur_len]]), valuestr)
				insert_db(shared_table.value, post)
				uidset.pop(lastkey[cur_len])
				valueset.pop(lastkey[cur_len])
				lastkey[cur_len] = s
			region_len -= 1

	for i in range(batch_len, 0, -1):
		vv = [[float(v) for v in vs.split()] for vs in valueset[lastkey[cur_len]]]
		vv_t = map(list, zip(*vv)) # transform
		v_sum = [str(sum(vt)) for vt in vv_t]
		valuestr = ' '.join(v_sum)
		yield "%s\t%d %s" % (lastkey[cur_len], len(uidset[lastkey[cur_len]]), valuestr)
		post = "%s\t%d %s" % (lastkey[cur_len], len(uidset[lastkey[cur_len]]), valuestr)
		insert_db(shared_table.value, post)
# -- top_down --

# -- navie_reducer --
def navie_reducer(data):
	pass
# -- navie_reducer --

def broadcast_var(table, hierarchy_col, common_col, value_col, id_col):
	global shared_table
	global shared_batch_dict
	global shared_batch
	global shared_value_col
	global shared_id_col	
	batch = get_C(hierarchy_col, common_col)
	batch_dict = get_batch_len(batch)
	shared_table = sc.broadcast(table)
	shared_batch = sc.broadcast(batch)
	shared_batch_dict = sc.broadcast(batch_dict)
	shared_value_col = sc.broadcast(value_col)
	shared_id_col = sc.broadcast(id_col)


def deal_opt(argv):
	table = ''
	hierarchy_col = []
	common_col = []
	value_col = []
	id_col = 0
	usage = 'Usage: spark-submit main.py -t <table_name>\n\t\t\t -l <hierarchy_col>\n\t\t\t-c <common_col>\n\t\t\t-v <value_col>\n\t\t\t -i <id_col>\n eg: spark-submit main.py -l 1,2,,3,4 -c 5,6,7 -v 8,9 -i 0'
	try:
		opts, args = getopt.getopt(argv, 'ht:l:c:v:i:')
	except getopt.GetoptError:
		print usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print usage
			sys.exit(1)
		elif opt == '-t':
			table = arg
		elif opt == '-l':
			hierarchy_col = [(int(t.split(',')[0]), int(t.split(',')[1])) for t in arg.split(',,')]
		elif opt == '-c':
			common_col = [int(t) for t in arg.split(',')]
		elif opt == '-v':
			value_col = [int(t) for t in arg.split(',')]
		elif opt == '-i':
			id_col = int(arg)
	return table, hierarchy_col, common_col, value_col, id_col

def main_func(sc, argv):
	table, hierarchy_col, common_col, value_col, id_col = deal_opt(argv)
	if __name__ == '__main__':
		broadcast_var(table, hierarchy_col, common_col, value_col, id_col)
	rdd = sc.textFile("file:///Users/liucancheng/Documents/GitHub/sparkDataCube/getData/" + table + "_100.txt")
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


#python test.py -l 1,2,,3,4 -c 5,6,7 -v 8,9 -i 0
#spark-submit main.py -l 1,2 -c 3,4 -v 5,6 -i 0


# -- navie_reducer --
if __name__ == "__main__":
	print "#####################################"
	sc = SparkContext(appName="Co-occurrence")	
	main_func(sc, sys.argv[1:])
	print "888888888888888888888888888888888888888888888888888888888888888888888888888888888888888"


