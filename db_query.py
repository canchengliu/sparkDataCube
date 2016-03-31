#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import pymongo
import sys
"""
# ('1', u'1 2|C Car\t1 10 120'),
# ('1', u'1 2 4|C cat #\t1 12 110'),
# ('1', u'1 2|C cat\t1 12 110'),
post = {'table_name':'userTable', 'hierarchy_col':'4,6,,7,8', 'common_col':'3', 'value_col':'9,10', 'id_col': '1', 'col_info': '0 rid:记录ID ［忽略字段］\n1 user_iD:用户ID ［ID字段］\n2 user_name:用户名，用户昵称 ［忽略字段］\n3 sex:用户性别(0代表男性，1代表女性) ［普通字段］\n4 country:用户居住国家 ［层级A字段］\n5 province:用户居住省份市 ［层级A字段］\n6 city:用户居住城市 ［层级A字段］\n7 favorite_category:最喜欢的商品类目 ［层级B字段］\n8 favorite_product:最喜欢的商品(限定为最喜欢的商品类目下的商品) ［层级B字段］\n9 month_visit:平均每年在该网站上购物消费总金额(元) ［度量字段］\n10 month_buy:平均每年在该网站上购物消费总金额(元) ［度量字段］\n'}

#spark-submit main.py -t userTable -l 4,6,,7,8 -c 3 -v 9,10 -i 1 > tmp
#spark-submit main.py -t productTable -l 4,6,,7,8 -c 3 -v 9,10 -i 1 > tmp
#
#tableinfo {'table_name':'userTable', 'hierarchy_col':'1,2,,3,4', common_col:'5,6', 'value_col':'7,8', 'id_col': '1', 'col_info'}
#userTable {''}

'''
'val': ['col': 'col', 'id': 'id']
'GROUP_BY': ['3', '4']
'WHERE': 'A=a and B=b or C=c and D=d or E=e and J=j or T=t'
'''


Select * from users WHERE age=33
db.users.find({age:33})
条件查询
Select a, b from users WHERE age=33
db.users.find({age:33},{a:1, b:1})
条件查询
select * from users WHERE age<33
db.users.find({'age':{$lt:33}})
条件查询
select * from users WHERE age>33 and age<=40
db.users.find({'age':{$gt:33,$lte:40}})
条件查询
select * from users WHERE a=1 and b='q'
db.users.find({a:1,b:'q'})
条件查询
select * from users WHERE a=1 or b=2
db.users.find( { $or : [ { a : 1 } , { b : 2 } ] } )

pti.find_one({ '$or' : [{'A':{'$gt': 'a'},'B':'b'}, {'C':'c','D':'d'}, {'E':'e','J':'j'}, {'T':'t'}]})


p = {'A':'a', 'B':'b', 'C':'c', 'D':'d', 'E':'e', 'J':'j', 'T':'t'}

ps = 'A=a and B=b or C=c and D=d or E=e and J=j or T=t'
pti.find_one({ '$or' : [{'A':{'$gt': 'a'},'B':'b'}, {'C':'c','D':'d'}, {'E':'e','J':'j'}, {'T':'t'}]})
"""
def get_col_info(table):
	client = pymongo.MongoClient('localhost',27017)
	tdb = client.testdb
	info_tbl = tdb.tableinfo
	col_info = info_tbl.find_one({'table_name': table})['col_info']
	info_dict = {}
	tups = [ci.split(':')[0].split(' ') for ci in col_info.split('\n')]
	for tup in tups:
		info_dict[tup[0]] = tup[1]
		info_dict[tup[1]] = tup[0]
	return info_dict
"""
def get_H_col(table, col_list):
	client = pymongo.MongoClient('localhost',27017)
	tdb = client.testdb
	info_tbl = tdb.tableinfo
	h_col_str = info_tbl.find_one({'table_name': table})['hierarchy_col']
	h_col = [(int(hc.split(',')[0]), int(hc.split(',')[0])) for hc in h_col_str.split(',,')]
	for c in col_list:
		for t1, t2 in h_col:
			if 
"""

def changeType(data):
	res = None
	if '.' in data:
		res = float(data)
	elif data.isdigit() == True:
		res = int(data)
	else:
		res = data
	return res

def deal_WHERE(data, info_dict):
	data.lower()
	p = {}
	or_list = []
	ps_or_part = data.split('OR')
	c_set = set()
	for ps_or in ps_or_part: # ps_or: A=a and B=b
		ps_dic = {}
		for ps in ps_or.split('AND'): # ps: A=a
			c = ''
			if '>=' in ps:
				c, v = ps.split('>=')
				v = changeType(v.strip())
				tmp = {}
				tmp['$gte'] = v
				c = info_dict[c.strip()]
				if c in ps_dic:
					ps_dic[c]['$gte'] = v
				else:
					ps_dic[c] = tmp
			elif '<=' in ps:
				c, v = ps.split('<=')
				v = changeType(v.strip())
				tmp = {}
				tmp['$lte'] = v
				c = info_dict[c.strip()]
				if c in ps_dic:
					ps_dic[c]['$lte'] = v
				else:
					ps_dic[c] = tmp
			elif '==' in ps:
				c, v = ps.split('==')
				v = changeType(v.strip())
				c = info_dict[c.strip()]
				ps_dic[c] = v
			elif '>' in ps:
				c, v = ps.split('>')
				v = changeType(v.strip())
				tmp = {}
				tmp['$gt'] = v
				c = info_dict[c.strip()]
				if c in ps_dic:
					ps_dic[c]['$gt'] = v
				else:
					ps_dic[c] = tmp
			elif '<' in ps:
				c, v = ps.split('<')
				v = changeType(v.strip())
				tmp = {}
				tmp['$lt'] = v
				c = info_dict[c.strip()]
				if c in ps_dic:
					ps_dic[c]['$lt'] = v
				else:
					ps_dic[c] = tmp
			c_set.add(c)
		or_list.append(ps_dic)

	if len(or_list) > 1:
		p['$or'] = or_list
	else:
		p = or_list[0]
	return (c_set, p)

#WHERE:string GROUP_BY:[string] value:[string]
#db.c1.find({x:{'$exists':true},b:{'$exists':true}, attr:3});
def db_query(table, json_query):
	info_dict = get_col_info(table)
	c_set, cond_dict = deal_WHERE(json_query['WHERE'], info_dict)
	group_list = [info_dict[c] for c in json_query['GROUP_BY']]
	for g in group_list:
		if g not in cond_dict:
			cond_dict[g] = {'$exists':True}
	col_list = list(group_list)
	value_list = [info_dict[c] for c in json_query['VALUE']]
	col_list.extend(list(c_set))
	col_list.extend(value_list)
	region = {}
	for col in group_list:
		region[col] = 1
	for col in value_list:
		region[col] = 1
	client = pymongo.MongoClient('localhost',27017)
	tdb = client.testdb
	tbl = tdb[table]
	res = tbl.find(cond_dict, region)
	#print cond_dict
	return res

def query_table(table):
	query_dict = {}
	query_dict['userTable'] = {'GROUP_BY': ['country', 'province', 'city'], 'WHERE': 'sex==0', 'VALUE': ['month_visit', 'month_buy']}
	query_dict['productTable'] = {'GROUP_BY': ['subcategory'], 'WHERE': 'subcategory < 3', 'VALUE': ['productID']}
	query_dict['saleTable'] = {'GROUP_BY': ['year', 'month'], 'WHERE': 'year>2007 OR month==3', 'VALUE': ['sales']}

	for q in  db_query(table, query_dict[table]):
		q.pop('_id')
		print q


if __name__ == '__main__':
	post_user = {'table_name':'userTable', 
		'hierarchy_col':'4,6,,7,8', 
		'common_col':'3', 
		'value_col':'9,10', 
		'id_col': '1', 
		'col_info': '0 rid:记录ID ［忽略字段］\n1 user_ID:用户ID ［ID字段］\n2 user_name:用户名，用户昵称 ［忽略字段］\n3 sex:用户性别(0代表男性，1代表女性) ［普通字段］\n4 country:用户居住国家 ［层级A字段］\n5 province:用户居住省份市 ［层级A字段］\n6 city:用户居住城市 ［层级A字段］\n7 favorite_category:最喜欢的商品类目 ［层级B字段］\n8 favorite_product:最喜欢的商品(限定为最喜欢的商品类目下的商品) ［层级B字段］\n9 month_visit:平均每年在该网站上购物消费总金额(元) ［度量字段］\n10 month_buy:平均每年在该网站上购物消费总金额(元) ［度量字段］'}
	
	post_product = {'table_name':'productTable', 
		'hierarchy_col':'2,3', 
		'common_col':'', 
		'value_col':'4,5', 
		'id_col': '1', 
		'col_info': '0 rid:记录ID ［忽略字段］\n1 productID:商品ID ［ID字段］\n2 category:商品所属一级类目 ［层级A字段］\n3 subcategory:商品所属二级类目 ［层级A字段］\n4 price:商品定价(元) ［度量字段］\n5 sales:商品月销量(件) ［度量字段］'}

	post_sale = {'table_name':'saleTable', 
		'hierarchy_col':'1,2,3', 
		'common_col':'', 
		'value_col':'4', 
		'id_col': '', 
		'col_info': '0 rid:记录ID ［忽略字段］\n1 year:年份 ［层级A字段］ \n2 month:月份 ［层级A字段］ \n3 day:日期 ［层级A字段］ \n4 sales:该网站单日销量交易额(万元) ［度量字段］'}
	query_table(sys.argv[1])










