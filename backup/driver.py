#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import sys
from main import *
from pyspark import SparkContext

def main_func(sc, rdd):
	print "!!!!!!!!!!!!!!!!!!!!!!\n!!!!!!!!!!!!!!!!!!!!!!!!!\n!!!!!!!!!!!!!!!!!!!!!!"
	print __name__
	if __name__ == '__main__':
		print "@@@@@@@@@@@\n@@@@@@@@@@@@@@\n@@@@@@@@@\n@@@@@@@@@@@@"
		hierarchy_col = [(1, 2)]
		common_col = [3, 4]
		value_col = [5, 6]
		id_col = 0
		#global shared_batch_dict
		#global shared_batch
		
		batch_dict = get_batch_len(batch)
		globalvar.shared_batch = sc.broadcast(batch)
		globalvar.shared_batch_dict = sc.broadcast(batch_dict)
	print "^^^^^^^^^^^^^^^^^^^\n^^^^^^^^^^^^\n^^^^^^^^^^^^^^^^^^^^^^^^\n^^^^^^^^^^^^^"

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
	print __name__
	print "#####################################"
	sc = SparkContext(appName="Co-occurrence")
	rdd = sc.textFile("file:///Users/liucancheng/Documents/GitHub/sparkDataCube/test.txt")
	main_func(sc, rdd)
	print "888888888888888888888888888888888888888888888888888888888888888888888888888888888888888"


