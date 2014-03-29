import os
import sys

if len(sys.argv) < 4:
    print 'usage: python remap-recordings.py file outfile device_a-device_b ...'
    sys.exit(1)

original = open(sys.argv[1], 'r')
out = open(sys.argv[2], 'w')

mappings = { }
mapping_strs = sys.argv[3:]
for s in mapping_strs:
    split_index = s.find('-')
    if split_index == -1:
        print 'Invalid mapping '+s
        sys.exit(1)
    a = s[:split_index]
    b = s[split_index+1:]
    a_i = int(a)
    b_i = int(b)
    if a_i in mappings.keys():
        print 'Duplicate mapping for '+str(a_i)
        sys.exit(1)

    mappings[a_i] = b_i
    print 'Mapping from '+str(a_i)+' -> '+str(b_i)

for line in original:
    tokens = line.split()
    curr = int(tokens[1])
    if curr in mappings.keys():
        tokens[1] = str(mappings[curr])
    out.write(' '.join(tokens)+'\n')

out.close()
original.close()
