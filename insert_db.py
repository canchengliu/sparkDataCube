#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import pymongo

# ('1', u'1 2|C Car\t1 10 120'),
# ('1', u'1 2 4|C cat #\t1 12 110'),
# ('1', u'1 2|C cat\t1 12 110'),
post = {'table_name':'userTable', 'hierarchy_col':'4,6,,7,8', 'common_col':'3', 'value_col':'9,10', 'id_col': '1', 'col_info': '0 rid:记录ID ［忽略字段］\n1 user_iD:用户ID ［ID字段］\n2 user_name:用户名，用户昵称 ［忽略字段］\n3 sex:用户性别(0代表男性，1代表女性) ［普通字段］\n4 country:用户居住国家 ［层级A字段］\n5 province:用户居住省份市 ［层级A字段］\n6 city:用户居住城市 ［层级A字段］\n7 favorite_category:最喜欢的商品类目 ［层级B字段］\n8 favorite_product:最喜欢的商品(限定为最喜欢的商品类目下的商品) ［层级B字段］\n9 month_visit:平均每年在该网站上购物消费总金额(元) ［度量字段］\n10 month_buy:平均每年在该网站上购物消费总金额(元) ［度量字段］\n'}

#spark-submit main.py -t userTable -l 4,6,,7,8 -c 3 -v 9,10 -i 1 > tmp
#spark-submit main.py -t productTable -l 4,6,,7,8 -c 3 -v 9,10 -i 1 > tmp
#
#tableinfo {'table_name':'userTable', 'hierarchy_col':'1,2,,3,4', common_col:'5,6', 'value_col':'7,8', 'id_col': '1', 'col_info'}
#userTable {''}
def insert_db(table, data):
	client = pymongo.MongoClient('localhost',27017)
	tdb = client.testdb
	info_tbl = tdb.tableinfo
	id_col = info_tbl.find_one({'table_name': table})['id_col']
	vc = info_tbl.find_one({'table_name': table})['value_col']
	vc_list = vc.split(',')
	
	record, value = data.split('\t')
	region, group = record.split('|')
	value_list = [float(v) for v in value.split()]
	value_dic = {}
	value_dic[id_col] = value_list[0]
	for i in range(len(vc_list)):
		value_dic[vc_list[i]] = value_list[i + 1]

	post = {}
	post['region'] = region
	post['group'] = group
	post['value'] = value_dic
	insert_tbl = tdb[table]
	insert_tbl.insert_one(post)