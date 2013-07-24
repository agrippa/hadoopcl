import sys
import os

fp = open('SupportedMR', 'r')

for line in fp:
    cmd = 'python AutoGenerateKernel.py '+line[0:len(line)-1]
    print cmd
    os.system(cmd)

print
arrayTypes = [ 'int', 'long', 'double', 'float' ]
for t in arrayTypes:
    cmd = 'python AutoGenerateArray.py '+t
    print cmd
    os.system(cmd)
