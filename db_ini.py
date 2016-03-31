#!/usr/bin/env python
# -*- coding: utf-8 -*- 

#spark-submit main.py -t userTable -l 4,6,,7,8 -c 3 -v 9,10 -i 1 > tmp
#spark-submit main.py -t productTable -l 2,3 -v 4,5 -i 1 > tmp
#spark-submit main.py -t saleTable -l 1,3 -v 4 > tmp

import pymongo
def db_ini():
	client = pymongo.MongoClient('localhost',27017)
	#if drop
	tdb = client.testdb

	post_u = {'name':'123', 'password': '123', 'context': 'hello'}
	tdb.user.insert_one(post_u)


	tdb.tableinfo.drop()
	info_tbl = tdb.tableinfo

	post_user = {'table_name':'userTable', 
			'hierarchy_col':'4,6,,7,8', 
			'common_col':'3', 
			'value_col':'9,10', 
			'id_col': '1', 
			'col_info': '0 rid:记录ID ［忽略字段］\n1 userID:用户ID ［ID字段］\n2 user_name:用户名，用户昵称 ［忽略字段］\n3 sex:用户性别(0代表男性，1代表女性) ［普通字段］\n4 country:用户居住国家 ［层级A字段］\n5 province:用户居住省份市 ［层级A字段］\n6 city:用户居住城市 ［层级A字段］\n7 favorite_category:最喜欢的商品类目 ［层级B字段］\n8 favorite_product:最喜欢的商品(限定为最喜欢的商品类目下的商品) ［层级B字段］\n9 month_visit:平均每年在该网站上购物消费总金额(元) ［度量字段］\n10 month_buy:平均每年在该网站上购物消费总金额(元) ［度量字段］'}

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

	info_tbl.insert_one(post_user)
	info_tbl.insert_one(post_product)
	info_tbl.insert_one(post_sale)
	#info_tbl.find_one({'table_name':'userTable'})

if __name__ == '__main__':
	db_ini()



