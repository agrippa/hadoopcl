import os
import sys

supportedTypes = [ 'int', 'float', 'double', 'long', 'pair', 'ipair', 'svec', 'ivec' ];

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
        elif typ == 'ivec':
            return self.processIvec()
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
    def processIvec(self):
        raise NotImplementedError("Unimplemented ivec processor")

class NativeToJavaVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        return typ.capitalize()
    def processPair(self):
        return 'Pair'
    def processIPair(self):
        return 'UPair'
    def processSvec(self):
        return 'Svec'
    def processIvec(self):
        return 'Ivec'

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
                'import java.util.HashMap;', 'import java.util.List;' ]
    def processIvec(self):
        return ['import org.apache.hadoop.io.HadoopCLResizableIntArray;',
                'import java.util.HashMap;', 'import java.util.List;' ]

class FieldDeclarationVisitor(IterTypeVisitor):
    def processPrimitive(self, typ):
        return [ '    private '+typ+'[] vals;', '    private int len;',
                 '    private int currentIndex;' ]
    def processPair(self):
        return [ '    private double[] vals1;', '    private double[] vals2;',
                 '    int len;', '    private int currentIndex;' ]
    def processIPair(self):
        return [ '    private int[] valIds;', '    private double[] vals1;',
                 '    private double[] vals2;', '    int len;',
                 '    private int currentIndex;' ]
    def processSvec(self):
        return [ '    private List<HadoopCLResizableIntArray> indices;',
                 '    private List<HadoopCLResizableDoubleArray> vals;',
                 '    private int currentIndex;', '    private int len;' ]
    def processIvec(self):
        return [ '    private List<HadoopCLResizableIntArray> vals;',
                 '    private int currentIndex;', '    private int len;' ]

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
        lines.append('    public HadoopCL'+NativeToJavaVisitor().process('svec')+
                """ValueIterator(List<HadoopCLResizableIntArray> indices,
                List<HadoopCLResizableDoubleArray> vals) {""")
        lines.append('        this.indices = indices;')
        lines.append('        this.vals = vals;')
        lines.append('        this.len = indices.size();')
        lines.append('        this.currentIndex = 0;')
        lines.append('    }')
        return lines
    def processIvec(self):
        lines = [ ]
        lines.append('    public HadoopCL'+NativeToJavaVisitor().process('ivec')+
                """ValueIterator(List<HadoopCLResizableIntArray> vals) {""")
        lines.append('        this.vals = vals;')
        lines.append('        this.len = vals.size();')
        lines.append('        this.currentIndex = 0;')
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
        lines.append('        return (int[])this.indices.get(this.currentIndex).getArray();')
        lines.append('    }')
        lines.append('')
        lines.append('    public double[] getValVals() {')
        lines.append('        return (double[])this.vals.get(this.currentIndex).getArray();')
        lines.append('    }')
        return lines
    def processIvec(self):
        lines = [ ]
        lines.append('    public int[] getArray() {')
        lines.append('        return (int[])this.vals.get(this.currentIndex).getArray();')
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
if typ == 'svec' or typ == 'ivec':
    fp.write('    public int vectorLength(int index) {\n')
    fp.write('        return this.indices.get(index).size();\n')
    fp.write('    }\n')
    fp.write('\n')
    fp.write('    public int currentVectorLength() {\n')
    fp.write('        return vectorLength(this.currentIndex);\n')
    fp.write('    }\n')
fp.write('}\n')

fp.close()
