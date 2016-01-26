#!/usr/bin/env python

import sys

uidset = {}
lastkey = [0,0,0,0,0,0,0,0,0]
batch_len = 0
lastbatch_len = 0
flag = 0
#print "%s.%s\t%s %s" % (str(p), key, str(data[1]), str(batch_len))
#print "|parition|.|record_segment|\t|ID| |batch_len|" % (str(p), key, str(data[1]), str(batch_len))

# batch_len : the number of region of the same batch, 
# which means those region belonging the same batch can 
# be solved in the same top-down prouach.

for line in sys.stdin:
    line = line.strip()
    s = line.split('\t')
    rvalue = s[0] #|parition|.|record_segment|
    uid = s[1].split() #[|ID|, |batch_len|] 
    ids = uid[0] #|ID|
    batch_len = int(uid[1]) #int(|batch_len|)
    if lastbatch_len == 0:
        lastbatch_len = batch_len
    if lastbatch_len != batch_len: #batch_len changed and not the first => flag = 1
        flag = 1
        lastbatch_len = batch_len
    rvalue = rvalue.split('|') # [|parition|.|region|, |group|]
    temp = rvalue[0].split('.') # [|parition|, |region|]
    region = temp[1].split() # reigon_list
    group  = rvalue[1].split() # group_list
    r_len = len(region) # region_len
    while batch_len != 0:
        s = ' '.join(region[0:r_len]) + '|' + ' '.join(group[0:r_len])
        if uidset.get(s,'none') == 'none':
            uidset[s] = set()
        uidset[s].add(ids) # group s add an id_list
        if lastkey[batch_len] == 0: # the first record of different region within the batch
            lastkey[batch_len] = s
        if lastkey[batch_len] != s: # group change
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



