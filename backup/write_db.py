#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import sys
import os
from insert_db import insert_db

def write_db(table):
	path = "/Users/liucancheng/Documents/GitHub/sparkDataCube/Data/%s" % table
	files = os.listdir(path)
	fname = ''
	lines = []
	for f in files:
		if f[0] == 'p':
			fname = path + '/' + f
			fin = open(fname)
			lines.extend(fin.readlines())
	insert_db(table, lines)

if __name__ == '__main__':
	write_db(sys.argv[1])