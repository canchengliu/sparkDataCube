#-- mapper.py --

#!/usr/bin/env python
import sys
from itertools import product


def seq(start, end):
    return [range(start, i) for i in range(start, end + 2)]


def read_input(file):
    for line in file:
        yield line.split()


def main():
    data = read_input(sys.stdin)
    C = [a + b for a, b in product(seq(2, 4), seq(5, 7))]
    for e in data:
        for R in C:
            k = [e[i] for i in R]
            print "%s|%s\t%s" % (' '.join([str(i) for i in R]), ' '.join(k), e[1])

if __name__ == "__main__":
    main()

#-- reducer.py --

#!/usr/bin/env python

from itertools import groupby
from operator import itemgetter
import sys


def read_input(file):
    for line in file:
        yield line.rstrip().split('\t')


def main():
    data = read_input(sys.stdin)
    for key, group in groupby(data, itemgetter(0)):
        ids = set(uid for key, uid in group)
        print "%s\t%d" % (key, len(ids))

if __name__ == "__main__":
    main()