import os
import sys

if len(sys.argv) < 4:
    print 'usage: python remap-launches.py file outfile device_a-device_b ...'
    sys.exit(1)

original = open(sys.argv[1], 'r')
out = open(sys.argv[2], 'w')

mapped_to = [ ]
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
    if b_i in mapped_to:
        print 'Multipled devices mapping to '+str(b_i)

    mappings[a_i] = b_i
    mapped_to.append(b_i)
    print 'Mapping from '+str(a_i)+' -> '+str(b_i)

for line in original:
    tokens = line.split()
    curr = int(tokens[1])
    if curr in mappings.keys():
        tokens[1] = str(mappings[curr])

    open_brace_index = 0
    while tokens[open_brace_index] != '[':
        open_brace_index = open_brace_index + 1
    close_brace_index = len(tokens) - 1

    curr_values = [ ]
    for i in range(0, close_brace_index - open_brace_index - 1):
        curr_values.append(int(tokens[open_brace_index + 1 + i]))

    for i in range(0, close_brace_index - open_brace_index - 1):
        if i in mappings.keys():
            tokens[open_brace_index + 1 + i] = str(curr_values[mappings[i]])
    
    out.write(' '.join(tokens)+'\n')

out.close()
original.close()
