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
    
def main():
    for line in sys.stdin:
        line = line.strip()
        data = line.split()
        C = [[[2, 3, 4, 5, 6, 7], [2, 3, 4, 5, 6], [2, 3, 4, 5], [2, 3, 4], [2, 3], [2]], [[2, 3, 5, 6, 7], [2, 3, 5, 6], [2, 3, 5]], [[2, 5, 6, 7], [2, 5, 6], [2, 5]], [[5, 6, 7], [5, 6], [5]]]
        for R in C:
            k = [data[i] for i in R[0]]
            key = ' '.join([str(i) for i in R[0]])+'|'+' '.join(k)
            p = BinarySearch(boundaries, key)
            batch_len = len(R)
            print "%s.%s\t%s %s" % (str(p), key, str(data[1]), str(batch_len))

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

if __name__ == "__main__":
    main()









