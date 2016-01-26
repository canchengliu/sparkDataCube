#!/usr/bin/env python

import sys

last_group = ""
ids = set()
for line in sys.stdin:
    line = line.strip()
    s = line.split('\t')
    group = s[0]
    cur_ids = set(s[1].split())
    if last_group == "":
        last_group = group
        ids = cur_ids
    elif last_group == group:
        ids = ids.union(cur_ids)
    else:
        print "%s\t%d" % (last_group, len(ids))
        last_group = group
        ids = cur_ids
print "%s\t%d" % (last_group, len(ids))
