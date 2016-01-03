#!/usr/bin/env python

import sys
import os

#t = int(os.environ.get('num_partitions'))
partition_file = open('part-00000')
boundaries = []
for item in partition_file:
    boundaries.append(item.strip())
t = len(boundaries)+1
partition_file.close()

def seq(start, end):
    return [range(start, i) for i in range(start, end + 2)]

def read_input(file):
    for line in file:
        yield line.split()

def get_C():
    data = read_input(sys.stdin)
    C = [a + b for a, b in product(seq(2, 4), seq(5, 7))]
    return C

def BinarySearch(array, target):
    low = 0
    height = t-2
    while low < height:
        mid = (low+height)/2
        if array[mid] < target:
            low = mid + 1
        elif array[mid] > target:
            height = mid - 1
        else:
            return mid+1
    return low

def reduce_materiliaze():
	uidset = {}
	lastkey = [0,0,0,0,0,0,0,0,0]
	batch_len = 0
	lastbatch_len = 0
	flag = 0
	#print "%s.%s\t%s %s" % (str(p), key, str(data[1]), str(batch_len))
	#print "|parition|.|record_segment|\t|ID| |batch_len|" % (str(p), key, str(data[1]), str(batch_len))
	for line in sys.stdin:
	    line = line.strip()
	    s = line.split('\t')
	    rvalue = s[0] #|parition|.|record_segment|
	    uid = s[1].split() 
	    ids = uid[0] #|ID|
	    batch_len = int(uid[1]) #|batch_len|
	    if lastbatch_len == 0:
	        lastbatch_len = batch_len
	    if lastbatch_len != batch_len:
	        flag = 1
	        lastbatch_len = batch_len
	    rvalue = rvalue.split('|')
	    temp = rvalue[0].split('.')
	    region = temp[1].split()
	    group  = rvalue[1].split()
	    r_len = len(region)
	    while batch_len != 0:
	        s = ' '.join(region[0:r_len]) + '|' + ' '.join(group[0:r_len])
	        if uidset.get(s,'none') == 'none': # dic uidset has not key ${s}
	            uidset[s] = set()
	        uidset[s].add(ids)
	        if lastkey[batch_len] == 0:
	            lastkey[batch_len] = s
	        if lastkey[batch_len] != s:
	            uidstr = ' '.join(uidset[lastkey[batch_len]])
	            if flag == 0:
	                print "%s\t%s" % (lastkey[batch_len], uidstr)
	            else:
	                pass
	            uidset.pop(lastkey[batch_len])
	            lastkey[batch_len] = s
	        batch_len = batch_len - 1
	        r_len = r_len - 1
	    flag = 0
	for i in range(1,lastbatch_len+1):
	    uidstr = ' '.join(uidset[lastkey[i]])
	    print "%s\t%s" % (lastkey[i], uidstr)
    
def main():
    for line in sys.stdin:
        line = line.strip()
        data = line.split()
        C = get_C()
        for R in C:
            k = [data[i] for i in R[0]]
            key = ' '.join([str(i) for i in R[0]])+'|'+' '.join(k)
            p = BinarySearch(boundaries, key)
            batch_len = len(R)
            print "%s.%s\t%s %s" % (str(p), key, str(data[1]), str(batch_len))

if __name__ == "__main__":
    main()
