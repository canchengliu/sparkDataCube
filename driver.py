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
	return hierarchy_col, common_col, value_col, id_col

if __name__ == '__main__':
	table = sys.argv[1]
	h, c, v, i = getTableInfo(table)
	ss = 'spark-submit main.py -t %s -l %s -c %s -v %s -i %s' % (table, h, c, v, i)
	os.system(ss)

