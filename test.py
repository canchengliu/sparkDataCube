#!/usr/bin/python

import sys, getopt


#python test.py -l 1,2,,3,4 -c 5,6,7 -v 8,9 -i 0

def deal_opt(argv):
	hierarchy_col = []
	common_col = []
	value_col = []
	id_col = 0
	usage = 'Usage: spark-submit main.py -l <hierarchy_col>\n\t\t\t-c <common_col>\n\t\t\t-v <value_col>\n\t\t\t -i <id_col>\n eg: spark-submit main.py -l 1,2,,3,4 -c 5,6,7 -v 8,9 -i 0'
	try:
		opts, args = getopt.getopt(argv, 'hl:c:v:i:')
	except getopt.GetoptError:
		print usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print usage
			sys.exit(1)
		elif opt == '-l':
			hierarchy_col = [(int(t.split(',')[0]), int(t.split(',')[1])) for t in arg.split(',,')]
		elif opt == '-c':
			common_col = [int(t) for t in arg.split(',')]
		elif opt == '-v':
			value_col = [int(t) for t in arg.split(',')]
		elif opt == '-i':
			id_col = int(arg)
	return hierarchy_col, common_col, value_col, id_col

if __name__ == "__main__":
	hierarchy_col, common_col, value_col, id_col = deal_opt(sys.argv[1:])







