
import sys
import os

fp = open('SupportedMR', 'r')

def typeNameForClassName(type):
    if type == 'ipair':
        return 'UPair'
    else:
        return type.capitalize()

for line in fp:
    tokens = line.split() 

    baseName = typeNameForClassName(tokens[1])+typeNameForClassName(tokens[2])+typeNameForClassName(tokens[3])+typeNameForClassName(tokens[4])
    kernelName = baseName+'HadoopCL'+tokens[0].capitalize()+'Kernel'
    bufferName = baseName + 'HadoopCL'+tokens[0].capitalize()+'Buffer'

    kernelFileName = 'mapred/org/apache/hadoop/mapreduce/'+kernelName+'.java'
    bufferFileName = 'mapred/org/apache/hadoop/mapreduce/'+bufferName+'.java'

    if os.path.exists(kernelFileName):
        cmd = 'rm '+kernelFileName
        print cmd
        os.system(cmd)

    if os.path.exists(bufferFileName):
        cmd = 'rm '+bufferFileName
        print cmd
        os.system(cmd)

arrayTypes = [ 'int', 'long', 'double', 'float' ]
for t in arrayTypes:
    arrayFileName = 'core/org/apache/hadoop/io/HadoopCLResizable'+t.capitalize()+'Array.java'
    if os.path.exists(arrayFileName):
        cmd = 'rm '+arrayFileName
        print cmd
        os.system(cmd)