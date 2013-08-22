import sys
import os

fp = open('SupportedMR', 'r')

generatedIters = [ ]

for line in fp:
    cmd = 'python AutoGenerateKernel.py '+line[0:len(line)-1]
    print cmd
    os.system(cmd)

    tokens = line.split()
    if tokens[0] == 'reducer':
        outputValType = tokens[2]
        if not outputValType in generatedIters:
            generatedIters.append(outputValType)
print
for outputValType in generatedIters:
    cmd = 'python AutoGenerateIter.py '+outputValType
    print cmd
    os.system(cmd)
print
arrayTypes = [ 'int', 'long', 'double', 'float' ]
for t in arrayTypes:
    cmd = 'python AutoGenerateArray.py '+t
    print cmd
    os.system(cmd)
