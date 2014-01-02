#!/usr/bin/python
import os
import sys

def findFirstNonWhiteSpace(line):
    firstNonWhite = 0
    while firstNonWhite < len(line) and (line[firstNonWhite] == ' ' or line[firstNonWhite] == '\t'):
        firstNonWhite = firstNonWhite + 1
    return firstNonWhite

# Just a minor extra safety check, and to avoid spurious changes to modified times
def checkForAnySpecialComments(filename):
    input = open(filename, 'r')

    for line in input:
        firstNonWhite = findFirstNonWhiteSpace(line)
        if firstNonWhite < len(line) - 5 and line[firstNonWhite:firstNonWhite+6] == '// LOG':
            input.close()
            return True

    input.close()
    return False

def processFile(filename, comment, tag):

    if not checkForAnySpecialComments(filename):
        return

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

if len(sys.argv) != 4:
    print 'usage: jpp.py <comment|uncomment> <PROFILE|DIAGNOSTIC> <file-name>'
    sys.exit()

comment = (sys.argv[1] == 'comment')
tag = sys.argv[2]
filename = sys.argv[3]

if os.path.isfile(filename):
    processFile(filename, comment, tag)
else:
    for root, dirnames, filenames in os.walk(filename):
        for f in filenames:
            processFile(root+'/'+f, comment, tag)

