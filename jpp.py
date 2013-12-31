#!/usr/bin/python
import os
import sys

def findFirstNonWhiteSpace(line):
    firstNonWhite = 0
    while firstNonWhite < len(line) and (line[firstNonWhite] == ' ' or line[firstNonWhite] == '\t'):
        firstNonWhite = firstNonWhite + 1
    return firstNonWhite

if len(sys.argv) != 4:
    print 'usage: jpp.py <comment|uncomment> <PROFILE|DIAGNOSTIC> <file-name>'
    sys.exit()

comment = (sys.argv[1] == 'comment')
tag = sys.argv[2]
filename = sys.argv[3]
input = open(filename, 'r')
output = open(filename+'.proc', 'w')

line = input.readline()
while len(line) > 0:
    line = line[0:len(line)-1]

    firstNonWhite = findFirstNonWhiteSpace(line)

    output.write(line+'\n')
    
    if firstNonWhite < len(line) - 5:
        if line[firstNonWhite:firstNonWhite+6] == '// LOG':
            thisTag = line[firstNonWhite+7:]

            if thisTag == tag:
                nextLine = input.readline()
                nextLine = nextLine[:len(nextLine)-1]
                firstNonWhite = findFirstNonWhiteSpace(nextLine)
                hasComment = firstNonWhite < len(nextLine)-1 and nextLine[firstNonWhite:firstNonWhite+3] == '// '
                if comment and not hasComment:
                    nextLine = nextLine[:firstNonWhite] + '// ' + nextLine[firstNonWhite:]
                elif not comment and hasComment:
                    nextLine = nextLine[:firstNonWhite] + nextLine[firstNonWhite+3:]
                output.write(nextLine+'\n')

    line = input.readline()

input.close()
output.close()

os.rename(filename+'.proc', filename)
