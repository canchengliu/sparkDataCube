#!/usr/bin/env python

import sys
import os

t = int(os.environ.get('num_partitions'))   # the number of partitions
#t = 10   # the number of partitions
sum = 0   # the total number of records from all mappers
interval = 1   #  sampling interval
count = 0
flag = 0
i = -1
for line in sys.stdin: # the result of map will be sent to the buffer of reducer and use merge sort to combine
    if line[0] == '0': # affer the shuffle stage, it comes first
        sum += int(line)
    elif flag == 0:
        i += 1
        flag = 1
        interval = sum/t
    else:
        i = (i+1)%interval
        if count < t-1 and i == interval-1: # t-1 split point, interval - 1 : reducer process size
            count += 1
            print line.strip('\n')
