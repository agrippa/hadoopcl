import os
import sys

validTypes = [ 'int', 'long', 'double', 'float' ]

def typeToSize(t):
    if t == 'int':
        return 4
    elif t == 'long':
        return 8
    elif t == 'double':
        return 8
    elif t == 'float':
        return 4
    else:
        print('Getting size for invalid type "'+t+'"')
        sys.exit(1)

if len(sys.argv) != 2:
    print 'usage: python AutoGenerateArray.py type'
    sys.exit()

type = sys.argv[1]
if not type in validTypes:
    print 'Invalid type '+type+', valid types include '+str(validTypes)
    sys.exit()

className = 'HadoopCLResizable'+type.capitalize()+'Array'
fp = open('core/org/apache/hadoop/io/'+className+'.java', 'w')

fp.write('package org.apache.hadoop.io;\n')
fp.write('\n')
fp.write('public class '+className+' implements HadoopCLResizableArray {\n')
fp.write('    private '+type+'[] buffer;\n')
fp.write('    private int size;\n')
fp.write('\n')
fp.write('    public '+className+'() {\n')
fp.write('        buffer = new '+type+'[512];\n')
fp.write('        size = 0;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public '+className+'(int initLength) {\n')
fp.write('        buffer = new '+type+'[initLength];\n')
fp.write('        size = 0;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public void reset() { size = 0; }\n')
fp.write('    public void forceSize(int s) { this.size = s; }\n')
fp.write('\n')
fp.write('    public void addAll('+type+'[] other, int N) {\n')
fp.write('        int newBufferLength = this.buffer.length;\n')
fp.write('        while(newBufferLength < N) { newBufferLength *= 2; }\n')
fp.write('        if(newBufferLength > buffer.length) {\n')
fp.write('            buffer = new '+type+'[newBufferLength];\n')
fp.write('        }\n')
fp.write('\n')
fp.write('        System.arraycopy(other, 0, this.buffer, 0, N);\n')
fp.write('        this.size = N;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public void add('+type+' val) {\n')
fp.write('        if(size == buffer.length) {\n')
fp.write('            '+type+'[] tmp = new '+type+'[buffer.length * 2];\n')
fp.write('            System.arraycopy(buffer, 0, tmp, 0, buffer.length);\n')
fp.write('            buffer = tmp;\n')
fp.write('        }\n')
fp.write('        buffer[size] = val;\n')
fp.write('        size = size + 1;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public void set(int index, '+type+' val) {\n')
fp.write('        ensureCapacity(index+1);\n')
fp.write('        unsafeSet(index, val);\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public void unsafeSet(int index, '+type+' val) {\n')
fp.write('        buffer[index] = val;\n')
fp.write('        size = (index + 1 > size ? index + 1 : size);\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public '+type+' get(int index) {\n')
fp.write('        return this.buffer[index];\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public Object getArray() { return buffer; }\n')
fp.write('    public int size() { return size; }\n')
fp.write('    public int length() { return buffer.length; }\n');
fp.write('\n')
fp.write('    public void copyTo(HadoopCLResizableArray other) {\n')
fp.write('        HadoopCLResizable'+type.capitalize()+'Array actual = (HadoopCLResizable'+type.capitalize()+'Array)other;\n')
fp.write('        actual.addAll(this.buffer, size);\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public void ensureCapacity(int size) {\n')
fp.write('        if(buffer.length < size) {\n')
fp.write('            Runtime r = Runtime.getRuntime();\n')
fp.write('            int n = (int)(buffer.length * 1.3);\n')
fp.write('            n = (n > size ? n : size);\n')
fp.write('            '+type+'[] tmp = new '+type+'[n];\n')
fp.write('            System.arraycopy(buffer, 0, tmp, 0, buffer.length);\n')
fp.write('            buffer = tmp;\n')
fp.write('        }\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public long space() {\n')
fp.write('        return this.buffer.length * '+str(typeToSize(type))+';\n')
fp.write('    }\n')
fp.write('\n')
fp.write('}\n')

fp.close()
