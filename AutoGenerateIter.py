import os
import sys

supportedTypes = [ 'int', 'float', 'double', 'long', 'pair', 'ipair', 'svec' ];

class IterTypeVisitor:
    def write(self, typ, fp):
        for line in self.process(typ):
            fp.write(line+'\n')

    def process(self, typ):
        if typ == 'ipair':
            return self.processIPair()
        elif typ == 'pair':
            return self.processPair()
        elif typ == 'svec':
            return self.processSvec()
        else:
            try:
                return self.processPrimitive(typ)
            except NotImplementedError:
                if typ == 'int':
                    return self.processInt()
                elif typ == 'float':
                    return self.processFloat()
                elif typ == 'double':
                    return self.processDouble()
                elif typ == 'long':
                    return self.processLong()
                else:
                    raise RuntimeError("Unsupported type "+typ)


    def processPrimitive(self, typ):
        raise NotImplementedError("Unimplemented primitive processor")
    def processInt(self):
        raise NotImplementedError("Unimplemented int processor")
    def processFloat(self):
        raise NotImplementedError("Unimplemented float processor")
    def processDouble(self):
        raise NotImplementedError("Unimplemented double processor")
    def processLong(self):
        raise NotImplementedError("Unimplemented long processor")

    def processPair(self):
        raise NotImplementedError("Unimplemented pair processor")
    def processIPair(self):
        raise NotImplementedError("Unimplemented ipair processor")
    def processSvec(self):
        raise NotImplementedError("Unimplemented svec processor")

class NativeToJavaVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        return typ.capitalize()
    def processPair(self):
        return 'Pair'
    def processIPair(self):
        return 'UPair'
    def processSvec(self):
        return 'Svec'

class ImportVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        return ['import org.apache.hadoop.io.HadoopCLResizable'+typ.capitalize()+'Array;' ]
    def processPair(self):
        return ['import org.apache.hadoop.io.HadoopCLResizableDoubleArray;' ]
    def processIPair(self):
        return ['import org.apache.hadoop.io.HadoopCLResizableIntArray;', 
                'import org.apache.hadoop.io.HadoopCLResizableDoubleArray;' ]
    def processSvec(self):
        return ['import org.apache.hadoop.io.HadoopCLResizableIntArray;',
                'import org.apache.hadoop.io.HadoopCLResizableDoubleArray;',
                'import java.util.HashMap;' ]

class FieldDeclarationVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        return [ '    private '+typ+'[] vals;', '    private int len;', '    private int currentIndex;' ]
    def processPair(self):
        return [ '    private double[] vals1;', '    private double[] vals2;', '    int len;', '    private int currentIndex;' ]
    def processIPair(self):
        return [ '    private int[] valIds;', '    private double[] vals1;', '    private double[] vals2;', '    int len;', '    private int currentIndex;' ]
    def processSvec(self):
        return [ '    private int[] lookAside;', '    private int[] indices;', '    private double[] vals;', '    private int len;', '    private int auxLen;', '    private int currentIndex;', '    private HashMap<Integer, int[]> indicesCache;', 'private HashMap<Integer, double[]> valsCache;' ]

class ConstructorVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        lines = [ ]
        lines.append('    public HadoopCL'+NativeToJavaVisitor().process(typ)+'ValueIterator('+typ+'[] setVals, int setLen) {')
        lines.append('        this.vals = setVals; this.len = setLen;')
        lines.append('        this.currentIndex = 0;')
        lines.append('    }')
        return lines
    def processPair(self):
        lines = [ ]
        lines.append('    public HadoopCL'+NativeToJavaVisitor().process('pair')+'ValueIterator(double[] setVals1, double[] setVals2, int setLen) {')
        lines.append('        this.vals1 = setVals1; this.vals2 = setVals2; this.len = setLen;')
        lines.append('        this.currentIndex = 0;')
        lines.append('    }')
        return lines
    def processIPair(self):
        lines = [ ]
        lines.append('    public HadoopCL'+NativeToJavaVisitor().process('ipair')+'ValueIterator(int[] setValIds, double[] setVals1, double[] setVals2, int setLen) {')
        lines.append('        this.valIds = setValIds; this.vals1 = setVals1; this.vals2 = setVals2; this.len = setLen;')
        lines.append('        this.currentIndex = 0;')
        lines.append('    }')
        return lines
    def processSvec(self):
        lines = [ ]
        lines.append('    public HadoopCL'+NativeToJavaVisitor().process('svec')+'ValueIterator(int[] setLookAside, int[] setIndices, double[] setVals, int setLen, int setAuxLen) {')
        lines.append('        this.lookAside = setLookAside; this.indices = setIndices; this.vals = setVals; this.len = setLen; this.auxLen = setAuxLen;')
        lines.append('        this.currentIndex = 0;')
        lines.append('        this.indicesCache = new HashMap<Integer, int[]>();')
        lines.append('        this.valsCache = new HashMap<Integer, double[]>();')
        lines.append('    }')
        return lines

class GetterVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        lines = [ ]
        lines.append('    public '+typ+' get() {')
        lines.append('        return this.vals[this.currentIndex];')
        lines.append('    }')
        return lines
    def processPair(self):
        lines = [ ]
        lines.append('    public double getVal1() {')
        lines.append('        return this.vals1[this.currentIndex];')
        lines.append('    }')
        lines.append('')
        lines.append('    public double getVal2() {')
        lines.append('        return this.vals2[this.currentIndex];')
        lines.append('    }')
        return lines
    def processIPair(self):
        lines = [ ]
        lines.append('    public int getValId() {')
        lines.append('        return this.valIds[this.currentIndex];')
        lines.append('    }')
        lines.append('')
        lines.append('    public double getVal1() {')
        lines.append('        return this.vals1[this.currentIndex];')
        lines.append('    }')
        lines.append('')
        lines.append('    public double getVal2() {')
        lines.append('        return this.vals2[this.currentIndex];')
        lines.append('    }')
        return lines
    def processSvec(self):
        lines = [ ]
        lines.append('    public int[] getValIndices() {')
        lines.append('        if(indicesCache.containsKey(this.currentIndex)) {')
        lines.append('            return indicesCache.get(this.currentIndex);')
        lines.append('        }')
        lines.append('        int start = this.lookAside[this.currentIndex];')
        lines.append('        int end = (this.currentIndex == this.len-1 ? this.auxLen : this.lookAside[this.currentIndex+1]);')
        lines.append('        int[] indices = new int[end-start];')
        lines.append('        System.arraycopy(this.indices, start, indices, 0, end-start);')
        lines.append('        this.indicesCache.put(this.currentIndex, indices);')
        lines.append('        return indices;')
        lines.append('    }')
        lines.append('')
        lines.append('    public double[] getValVals() {')
        lines.append('        if(valsCache.containsKey(this.currentIndex)) {')
        lines.append('            return valsCache.get(this.currentIndex);')
        lines.append('        }')
        lines.append('        int start = this.lookAside[this.currentIndex];')
        lines.append('        int end = (this.currentIndex == this.len-1 ? this.auxLen : this.lookAside[this.currentIndex+1]);')
        lines.append('        double[] vals = new double[end-start];')
        lines.append('        System.arraycopy(this.vals, start, vals, 0, end-start);')
        lines.append('        this.valsCache.put(this.currentIndex, vals);')
        lines.append('        return vals;')
        lines.append('    }')
        return lines

if len(sys.argv) != 2:
    print 'usage: python AutoGenerateIter.py type'
    sys.exit(1)

typ = sys.argv[1]
if not typ in supportedTypes:
    print 'Unsupported type '+typ
    sys.exit(1)

fp = open('mapred/org/apache/hadoop/mapreduce/HadoopCL'+NativeToJavaVisitor().process(typ)+'ValueIterator.java', 'w')

fp.write('package org.apache.hadoop.mapreduce;\n')
fp.write('\n')
ImportVisitor().write(typ, fp)
fp.write('\n')
fp.write('public class HadoopCL'+NativeToJavaVisitor().process(typ)+'ValueIterator {\n')
FieldDeclarationVisitor().write(typ, fp)
fp.write('\n')
ConstructorVisitor().write(typ, fp)
fp.write('\n')
fp.write('    public boolean next() {\n')
fp.write('        if (this.currentIndex == this.len-1) return false;\n')
fp.write('        this.currentIndex = this.currentIndex + 1;\n')
fp.write('        return true;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public boolean seekTo(int set) {\n')
fp.write('        if (set >= this.len) return false;\n')
fp.write('        this.currentIndex = set;\n')
fp.write('        return true;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public int current() {\n')
fp.write('        return this.currentIndex;\n')
fp.write('    }\n')
fp.write('\n')
fp.write('    public int nValues() {\n')
fp.write('        return this.len;\n')
fp.write('    }\n')
fp.write('\n')
GetterVisitor().write(typ, fp)
fp.write('\n')
if typ == 'svec':
    fp.write('    public int vectorLength(int index) {\n')
    fp.write('        int start = this.lookAside[index];\n')
    fp.write('        int end = (index == this.len-1 ? this.auxLen : this.lookAside[index+1]);\n')
    fp.write('        return end-start;\n')
    fp.write('    }\n')
    fp.write('\n')
    fp.write('    public int currentVectorLength() {\n')
    fp.write('        return vectorLength(this.currentIndex);\n')
    fp.write('    }\n')
fp.write('}\n')

fp.close()
