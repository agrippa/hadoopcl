
import sys
import os

fp = open('SupportedMR', 'r')

generatedIters = [ ]

baseCmd = 'git rm -f '

def getCmd(filename):
    return 'git rm -f '+filename+'; rm -f '+filename

def typeNameForClassName(type):
    if type == 'ipair':
        return 'UPair'
    else:
        return type.capitalize()

for line in fp:
    tokens = line.split() 

    baseName = typeNameForClassName(tokens[1])+typeNameForClassName(tokens[2])+typeNameForClassName(tokens[3])+typeNameForClassName(tokens[4])
    kernelName = baseName+'HadoopCL'+tokens[0].capitalize()+'Kernel'
    oldBufferName = baseName + 'HadoopCL'+tokens[0].capitalize()+'Buffer'
    inputBufferName = typeNameForClassName(tokens[1])+typeNameForClassName(tokens[2]) + 'HadoopCLInput'+tokens[0].capitalize()+'Buffer'
    outputBufferName = typeNameForClassName(tokens[3])+typeNameForClassName(tokens[4]) + 'HadoopCLOutput'+tokens[0].capitalize()+'Buffer'

    kernelFileName = 'mapred/org/apache/hadoop/mapreduce/'+kernelName+'.java'
    oldBufferFileName = 'mapred/org/apache/hadoop/mapreduce/'+oldBufferName+'.java'
    inputBufferFileName = 'mapred/org/apache/hadoop/mapreduce/'+inputBufferName+'.java'
    outputBufferFileName = 'mapred/org/apache/hadoop/mapreduce/'+outputBufferName+'.java'

    if os.path.exists(kernelFileName):
        cmd = getCmd(kernelFileName)
        print cmd
        os.system(cmd)

    # This just cleans up the old buffer file if it still exists, i.e. for upgrades on existing systems
    if os.path.exists(oldBufferFileName):
        cmd = getCmd(oldBufferFileName)
        print cmd
        os.system(cmd)

    if os.path.exists(inputBufferFileName):
        cmd = getCmd(inputBufferFileName)
        print cmd
        os.system(cmd)

    if os.path.exists(outputBufferFileName):
        cmd = getCmd(outputBufferFileName)
        print cmd
        os.system(cmd)

    if tokens[0] == 'reducer':
        outputValType = tokens[2]
        if not outputValType in generatedIters:
            generatedIters.append(outputValType)
            iterName = 'HadoopCL'+typeNameForClassName(outputValType)+'ValueIterator'
            iterFileName = 'mapred/org/apache/hadoop/mapreduce/'+iterName+'.java'
            if os.path.exists(iterFileName):
                cmd = getCmd(iterFileName)
                print cmd
                os.system(cmd)

arrayTypes = [ 'int', 'long', 'double', 'float' ]
for t in arrayTypes:
    arrayFileName = 'core/org/apache/hadoop/io/HadoopCLResizable'+t.capitalize()+'Array.java'
    if os.path.exists(arrayFileName):
        cmd = getCmd(arrayFileName)
        print cmd
        os.system(cmd)


