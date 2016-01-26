#!/usr/bin/env python

import sys
import os
import random
import math

C = [[[2, 3, 4, 5, 6, 7], [2, 3, 4, 5, 6], [2, 3, 4, 5], [2, 3, 4], [2, 3], [2]], [[2, 3, 5, 6, 7], [2, 3, 5, 6], [2, 3, 5]], [[2, 5, 6, 7], [2, 5, 6], [2, 5]], [[5, 6, 7], [5, 6], [5]]]
n = int(os.environ.get('num_lines'))
p = int(os.environ.get('num_partitions'))
m = n/p
lo = math.log(n*p)/m   # sampling proportion
#lo = 0.1   # sampling proportion
count = 0   # counter for the data line to be printed
for line in sys.stdin:
    line = line.strip()
    data = line.split()
    if random.random() <= lo:
        for R in C:
            count = count+1
            k = [data[i] for i in R[0]]
            print "%s|%s\t%s" % (' '.join([str(i) for i in R[0]]), ' '.join(k), str(data[1]))
print "0%s" % (str(count))
