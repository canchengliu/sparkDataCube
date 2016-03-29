#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import pymongo
import sys
import os
#spark-submit main.py -t userTable -l 4,6,,7,8 -c 3 -v 9,10 -i 1 > tmp
#spark-submit main.py -t productTable -l 2,3 -v 4,5 -i 1 > tmp
#spark-submit main.py -t saleTable -l 1,3 -v 4 > tmp
def getTableInfo(table):
	client = pymongo.MongoClient('localhost', 27017)
	tdb = client.testdb
	info_tbl = tdb.tableinfo
	hierarchy_col = info_tbl.find_one({'table_name': table})['hierarchy_col']
	common_col = info_tbl.find_one({'table_name': table})['common_col']
	value_col = info_tbl.find_one({'table_name': table})['value_col']
	id_col = info_tbl.find_one({'table_name': table})['id_col']
	#delete the old data cube of table
	tdb[table].remove({})
	return hierarchy_col, common_col, value_col, id_col

if __name__ == '__main__':
	table = sys.argv[1]
	h, c, v, i = getTableInfo(table)
	opt = ''
	if (len(h) > 0):
		opt += ' -l ' + h 
	if (len(c) > 0):
		opt += ' -c ' + c
	if (len(v) > 0):
		opt += ' -v ' + v
	if (len(i) > 0):
		opt += ' -i ' + i
	ss = 'spark-submit main.py -t ' + table + opt
	print ss
	os.system(ss)

