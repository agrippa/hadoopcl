import sys

supportedTypes = [ 'int', 'float', 'double', 'long', 'pair', 'ipair', 'svec' ];

def checkTypeSupported(type):
    return type in supportedTypes

def kernelString(isMapper):
    if isMapper:
        return 'mapper'
    else:
        return 'reducer'

def compactHadoopName(name):
    if name == 'UniquePair':
        return 'UPair'
    elif name == 'SparseVector':
        return 'Svec'
    else:
        return name

def isPrimitiveHadoopType(typeName):
    if typeName == 'Pair' or typeName == 'UniquePair' or typeName == 'SparseVector':
        return False
    return True

def capitalizedKernelString(isMapper):
    return kernelString(isMapper).capitalize()

def typeNameForClassName(t):
    if t == 'ipair':
        return 'UPair'
    elif t == 'svec':
        return 'Svec'
    else:
        return t.capitalize()

def typeNameForWritable(t):
    if t == 'ipair':
        return 'UniquePair'
    elif t == 'svec':
        return 'SparseVector'
    else:
        return t.capitalize()

def classNameHelper(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType, prefix, postfix):
    hadoopInputKeyType = typeNameForClassName(inputKeyType)
    hadoopInputValueType = typeNameForClassName(inputValueType)
    hadoopOutputKeyType = typeNameForClassName(outputKeyType)
    hadoopOutputValueType = typeNameForClassName(outputValueType)

    return (hadoopInputKeyType+hadoopInputValueType+hadoopOutputKeyType+
        hadoopOutputValueType+prefix+capitalizedKernelString(isMapper)+postfix)
    
def kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType):
    return classNameHelper(isMapper, inputKeyType, inputValueType, outputKeyType, 
            outputValueType, 'HadoopCL', 'Kernel')

def bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType):
    return classNameHelper(isMapper, inputKeyType, inputValueType, outputKeyType, 
            outputValueType, 'HadoopCL', 'Buffer')

def className(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType):
    return classNameHelper(isMapper, inputKeyType, inputValueType, outputKeyType, 
            outputValueType, 'OpenCL', 'Kernel')

def generateKeyValDecl(basename, type, fp, isInput):
    if type == 'pair':
        fp.write('    public double[] '+basename+'s1;\n')
        fp.write('    public double[] '+basename+'s2;\n')
    elif type == 'ipair':
        fp.write('    public int[] '+basename+'Ids;\n')
        fp.write('    public double[] '+basename+'s1;\n')
        fp.write('    public double[] '+basename+'s2;\n')
    elif type == 'svec':
        if isInput:
            fp.write('    public int[] '+basename+'LookAsideBuffer;\n')
        else:
            fp.write('    public int[] '+basename+'IntLookAsideBuffer;\n')
            fp.write('    public int[] '+basename+'DoubleLookAsideBuffer;\n')
        fp.write('    public int[] '+basename+'Indices;\n')
        fp.write('    public double[] '+basename+'Vals;\n')
    else:
        fp.write('    public '+type+'[] '+basename+'s;\n')

def generateKeyValInit(basename, type, fp, size, indent, forceNull, isInput):
    indentStr = ''
    if indent:
        indentStr = '    '
    if type == 'pair':
        if forceNull:
            initializer = 'null'
        else:
            initializer = 'new double['+size+']'
        fp.write(indentStr+'        '+basename+'s1 = '+initializer+';\n')
        fp.write(indentStr+'        '+basename+'s2 = '+initializer+';\n')
    elif type == 'ipair':
        if forceNull:
            initializer1 = 'null';
            initializer2 = 'null';
        else:
            initializer1 = 'new int['+size+']';
            initializer2 = 'new double['+size+']';
        fp.write(indentStr+'        '+basename+'Ids = '+initializer1+';\n')
        fp.write(indentStr+'        '+basename+'s1 = '+initializer2+';\n')
        fp.write(indentStr+'        '+basename+'s2 = '+initializer2+';\n')
    elif type == 'svec':
        if forceNull:
            if isInput:
                fp.write(indentStr+'        '+basename+'LookAsideBuffer = null;\n')
            else:
                fp.write(indentStr+'        '+basename+'IntLookAsideBuffer = null;\n')
                fp.write(indentStr+'        '+basename+'DoubleLookAsideBuffer = null;\n')
            fp.write(indentStr+'        '+basename+'Indices = null;\n')
            fp.write(indentStr+'        '+basename+'Vals = null;\n')
        else:
            if isInput:
                fp.write(indentStr+'        '+basename+'LookAsideBuffer = new int['+size+'];\n')
            else:
                fp.write(indentStr+'        '+basename+'IntLookAsideBuffer = new int['+size+'];\n')
                fp.write(indentStr+'        '+basename+'DoubleLookAsideBuffer = new int['+size+'];\n')
            fp.write(indentStr+'        '+basename+'Indices = new int[('+size+') * 5];\n')
            fp.write(indentStr+'        '+basename+'Vals = new double[('+size+') * 5];\n')
    else:
        if forceNull:
            initializer = 'null'
        else:
            initializer = 'new '+type+'['+size+']'
        fp.write(indentStr+'        '+basename+'s = '+initializer+';\n')


def generateKeyValSetup(basename, type, fp, isInput):
    if type == 'pair':
        fp.write('        this.'+basename+'s1 = set'+basename.capitalize()+'s1;\n')
        fp.write('        this.'+basename+'s2 = set'+basename.capitalize()+'s2;\n')
    elif type == 'ipair':
        fp.write('        this.'+basename+'Ids = set'+basename.capitalize()+'Ids;\n')
        fp.write('        this.'+basename+'s1 = set'+basename.capitalize()+'s1;\n')
        fp.write('        this.'+basename+'s2 = set'+basename.capitalize()+'s2;\n')
    elif type == 'svec':
        if isInput:
            fp.write('        this.'+basename+'LookAsideBuffer = set'+basename.capitalize()+'LookAsideBuffer;\n')
        else:
            fp.write('        this.'+basename+'IntLookAsideBuffer = set'+basename.capitalize()+'IntLookAsideBuffer;\n')
            fp.write('        this.'+basename+'DoubleLookAsideBuffer = set'+basename.capitalize()+'DoubleLookAsideBuffer;\n')
        fp.write('        this.'+basename+'Indices = set'+basename.capitalize()+'Indices;\n')
        fp.write('        this.'+basename+'Vals = set'+basename.capitalize()+'Vals;\n')
    else:
        fp.write('        this.'+basename+'s = set'+basename.capitalize()+'s;\n')

def generateKeyValAccess(basename, type, indexStr):
    return 'this.output'+basename+'s['+indexStr+']'

def generateKeyValSet(basename, type, indexStr, indent):
    if type == 'pair':
        return indent+'                    save'+basename+'.set(this.output'+basename+'s1['+indexStr+'], this.output'+basename+'s2['+indexStr+']);\n'
    elif type == 'ipair':
        return indent+'                    save'+basename+'.set(this.output'+basename+'Ids['+indexStr+'], this.output'+basename+'s1['+indexStr+'], this.output'+basename+'s2['+indexStr+']);\n'
    else:
        return indent+'                    save'+basename+'.set(this.output'+basename+'s['+indexStr+']);\n'

def generateWriteSig(outputKeyType, outputValType, fp):
    acc = '    protected boolean write('

    if outputKeyType == 'pair':
        acc = acc + 'double key1, double key2'
    elif outputKeyType == 'ipair':
        acc = acc + 'int keyId, double key1, double key2'
    elif outputKeyType == 'svec':
        print('Unsupported output key type svec')
        sys.exit(1)
    else:
        acc = acc + outputKeyType+' key'

    if outputValType == 'pair':
        acc = acc + ', double val1, double val2'
    elif outputValType == 'ipair':
        acc = acc + ', int valId, double val1, double val2'
    elif outputValType == 'svec':
        acc = acc + ', int[] valIndices, double[] valVals, int len'
    else:
        acc = acc + ', '+outputValType+' val'
    acc = acc + ') {\n'

    fp.write(acc)

def generateWriteMethod(fp, nativeOutputKeyType, nativeOutputValueType):
    generateWriteSig(nativeOutputKeyType, nativeOutputValueType, fp)
    fp.write('        this.javaProfile.stopKernel();\n')
    fp.write('        this.javaProfile.startWrite();\n')
    if nativeOutputKeyType == 'pair':
        fp.write('        keyObj.set(key1, key2);\n')
    elif nativeOutputKeyType == 'ipair':
        fp.write('        keyObj.set(keyId, key1, key2);\n')
    elif nativeOutputKeyType == 'svec':
        print('Unsupported output key type svec')
        sys.exit(1)
    else:
        fp.write('        keyObj.set(key);\n')

    if nativeOutputValueType == 'pair':
        fp.write('        valObj.set(val1, val2);\n')
    elif nativeOutputValueType == 'ipair':
        fp.write('        valObj.set(valId, val1, val2);\n')
    elif nativeOutputValueType == 'svec':
        fp.write('        valObj.set(valIndices, valVals, len);\n')
        #fp.write('        valObj.set(this.bufferOutputIndices, this.bufferOutputVals);\n')
    else:
        fp.write('        valObj.set(val);\n')
    fp.write('        try { clContext.getContext().write(keyObj, valObj); } catch(Exception ex) { throw new RuntimeException(ex); }\n')
    fp.write('        this.javaProfile.stopWrite();\n')
    fp.write('        this.javaProfile.startKernel();\n')
    fp.write('        return true;\n')
    fp.write('    }\n')
    fp.write('\n')

def generateFakeRead(basename, type, fp):
    if type == 'pair':
        fp.write('            double[] dummy'+basename+'1 = output'+basename+'s1;\n')
        fp.write('            double[] dummy'+basename+'2 = output'+basename+'s2;\n')
    elif type == 'ipair':
        fp.write('            int[] dummy'+basename+'1 = output'+basename+'Ids;\n')
        fp.write('            double[] dummy'+basename+'2 = output'+basename+'s1;\n')
        fp.write('            double[] dummy'+basename+'3 = output'+basename+'s2;\n')
    else:
        fp.write('            '+type+'[] dummy'+basename+' = output'+basename+'s;\n')


def generateWriteAssignment(basename, type, fp, index):

    if type == 'pair':
        fp.write('        output'+basename+'s1['+index+'] = '+basename.lower()+'1;\n')
        fp.write('        output'+basename+'s2['+index+'] = '+basename.lower()+'2;\n')
    elif type == 'ipair':
        fp.write('        output'+basename+'Ids['+index+'] = '+basename.lower()+'Id;\n')
        fp.write('        output'+basename+'s1['+index+'] = '+basename.lower()+'1;\n')
        fp.write('        output'+basename+'s2['+index+'] = '+basename.lower()+'2;\n')
    else:
        fp.write('        output'+basename+'s['+index+'] = '+basename.lower()+';\n')

def generateKernelDecl(isMapper, keyType, valType, fp):
    if isMapper:
        acc = '    protected abstract void map('
        if keyType == 'pair':
            acc = acc + 'double key1, double key2, '
        elif keyType == 'ipair':
            acc = acc + 'int keyId, double key1, double key2, '
        elif keyType == 'svec':
            print 'Invalid input key type for mapper: svec'
            sys.exit(1)
        else:
            acc = acc + keyType+' key, '

        if valType == 'pair':
            acc = acc + 'double val1, double val2'
        elif valType == 'ipair':
            acc = acc + 'int valId, double val1, double val2'
        elif valType == 'svec':
            acc = acc + 'int[] indices, double[] vals, int len'
            #acc = acc + 'int[] indices, double[] vals, int start, int end'
        else:
            acc = acc + valType+' val'

        acc = acc + ');\n'
        fp.write(acc)
    else:
        acc = '    protected abstract void reduce('

        if keyType == 'pair':
            acc = acc + 'double key1, double key2, '
        elif keyType == 'ipair':
            acc = acc + 'int keyId, double key1, double key2, '
        elif keyType == 'svec':
            print 'Invalid input key type for mapper: svec'
            sys.exit(1)
        else:
            acc = acc + keyType+' key, '

        if valType == 'pair':
            acc = acc + 'HadoopCLPairValueIterator valIter);'
        elif valType == 'ipair':
            acc = acc + 'HadoopCLUPairValueIterator valIter);'
        elif valType == 'svec':
            acc = acc + 'HadoopCLSvecValueIterator valIter);'
        else:
            acc = acc + 'HadoopCL'+valType.capitalize()+'ValueIterator valIter);'
        #if valType == 'pair':
        #    acc = acc + 'double[] vals1, double[] vals2'
        #elif valType == 'ipair':
        #    acc = acc + 'int[] valIds, double[] vals1, double[] vals2'
        #elif valType == 'svec':
        #    acc = acc + 'int[] valLookAsideBuffer, int[] valIndices, double[] valVals'
        #else:
        #    acc = acc + valType+'[] vals'
        #acc = acc + ', int startValueOffset, int stopValueOffset);\n'
        fp.write(acc)

def generateKernelCall(isMapper, keyType, valType, fp):
    fp.write('    @Override\n')
    if isMapper:
        fp.write('    protected void callMap() {\n')

        acc = '        map('

        if keyType == 'pair':
            acc = acc + 'inputKeys1[3], inputKeys2[3], '
        elif keyType == 'ipair':
            acc = acc + 'inputKeyIds[3], inputKeys1[3], inputKeys2[3], '
        elif keyType == 'svec':
            print 'Unsupported input key type svec'
            sys.exit(1)
        else:
            acc = acc + 'inputKeys[3], '

        if valType == 'pair':
            acc = acc + 'inputVals1[3], inputVals2[3]'
        elif valType == 'ipair':
            acc = acc + 'inputValIds[3], inputVals1[3], inputVals2[3]'
        elif valType == 'svec':
            #acc = acc + 'inputValIndices, inputValVals, inputValLookAsideBuffer[3], this.nPairs + this.individualInputValsCount';
            acc = acc + 'inputValIndices, inputValVals, inputValLookAsideBuffer[3] + this.nPairs + this.individualInputValsCount';
        else:
            acc = acc + 'inputVals[3]'

        acc = acc + ');\n'
        fp.write(acc)
        fp.write('    }\n')

    else:
        fp.write('    protected void callReduce(int startOffset, int stopOffset)  {\n')

        acc = '        reduce('

        if keyType == 'pair':
            acc = acc + 'inputKeys1[3], inputKeys2[3], '
        elif keyType == 'ipair':
            acc = acc + 'inputKeyIds[3], inputKeys1[3], inputKeys2[3], '
        else:
            acc = acc + 'inputKeys[3], '

        #if valType == 'pair':
        #    acc = acc + 'inputVals1, inputVals2, '
        #elif valType == 'ipair':
        #    acc = acc + 'inputValIds, inputVals1, inputVals2, '
        #elif valType == 'svec':
        #    acc = acc + 'inputValLookAsideBuffer, inputValIndices, inputValVals, '
        #else:
        #    acc = acc + 'inputVals, '
        #acc = acc + 'startOffset, stopOffset);\n'
        if valType == 'pair':
            acc = acc + 'new HadoopCLPairValueIterator(inputVals1, inputVals2, stopOffset-startOffset));\n'
        elif valType == 'ipair':
            acc = acc + 'new HadoopCLUPairValueIterator(inputValIds, inputVals1, inputVals2, stopOffset-startOffset));\n'
        elif valType == 'svec':
            # acc = acc + 'new HadoopCLSvecValueIterator(inputValLookAsideBuffer, inputValIndices, inputValVals, stopOffset-startOffset, 0));\n'
            acc = acc + 'new HadoopCLSvecValueIterator(null, null));\n'
        else:
            acc = acc + 'new HadoopCL'+valType.capitalize()+'ValueIterator(inputVals, stopOffset-startOffset));\n'

        fp.write(acc)
        fp.write('    }\n')

def writeHeader(fp, isMapper):
    fp.write('package org.apache.hadoop.mapreduce;\n')
    fp.write('\n')
    fp.write('import java.io.IOException;\n')
    fp.write('import java.lang.InterruptedException;\n')
    fp.write('import org.apache.hadoop.mapreduce.TaskInputOutputContext;\n')
    fp.write('import com.amd.aparapi.Range;\n')
    fp.write('import com.amd.aparapi.Kernel;\n')
    fp.write('import org.apache.hadoop.io.*;\n')
    fp.write('import java.util.List;\n')
    fp.write('import java.util.ArrayList;\n')
    fp.write('import java.util.HashMap;\n')
    if isMapper:
        fp.write('import org.apache.hadoop.mapreduce.Mapper.Context;\n')
    else:
        fp.write('import org.apache.hadoop.mapreduce.Reducer.Context;\n')
    fp.write('\n')

def writePopulatesMethod(fp, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void populate(Object genericReducerKeys, Object genericReducerValues, int[] keyIndex) {\n')
    if nativeOutputValueType == 'pair' or nativeOutputValueType == 'ipair' or nativeOutputValueType == 'svec':
        fp.write('        throw new RuntimeException("Invalid to call populate on mapper with non-primitive output");\n')
    else:
        fp.write('        int[] privateKeyIndex = new int[keyIndex.length];\n')
        fp.write('        System.arraycopy(keyIndex, 0, privateKeyIndex, 0, keyIndex.length);\n')
        fp.write('        '+nativeOutputKeyType+'[] reducerKeys = ('+nativeOutputKeyType+'[])genericReducerKeys;\n')
        fp.write('        '+nativeOutputValueType+'[] reducerValues = ('+nativeOutputValueType+'[])genericReducerValues;\n')
        fp.write('\n')
        
        firstLines = [ ]
        firstLines.append('                          int index = 0;\n')
        firstLines.append('                          while(reducerKeys[index] != this.outputKeys[DUMMY]) index++;\n')
        firstLines.append('                          reducerValues[privateKeyIndex[index]++] = this.outputVals[DUMMY];\n')
        
        secondLines = [ ]
        secondLines.append('                          int index = 0;\n')
        secondLines.append('                          while(reducerKeys[index] != this.outputKeys[DUMMY]) index++;\n')
        secondLines.append('                          reducerValues[privateKeyIndex[index]++] = this.outputVals[DUMMY];\n')
        
        writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLines, secondLines)
        
    fp.write('    }\n')

def writeKeyCountsMethod(fp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public HashMap getKeyCounts() {\n')
    if nativeOutputValueType == 'pair' or nativeOutputValueType == 'ipair' or nativeOutputValueType == 'svec':
        fp.write('        throw new RuntimeException("Invalid to call getKeyCounts on mapper with non-primitive output");\n')
    else:
        fp.write('        HashMap<'+hadoopOutputKeyType+'Writable, HadoopCLMutableInteger> keyCounts = new HashMap<'+hadoopOutputKeyType+'Writable, HadoopCLMutableInteger>();\n')
        
        firstLines = [ ]
        firstLines.append('                           '+hadoopOutputKeyType+'Writable temp = new '+hadoopOutputKeyType+'Writable(this.outputKeys[DUMMY]);\n')
        firstLines.append('                           if(!keyCounts.containsKey(temp)) keyCounts.put(temp, new HadoopCLMutableInteger());\n')
        firstLines.append('                           keyCounts.get(temp).incr();\n')
        
        secondLines = [ ]
        secondLines.append('                               '+hadoopOutputKeyType+'Writable temp = new '+hadoopOutputKeyType+'Writable(this.outputKeys[DUMMY]);\n')
        secondLines.append('                               if(!keyCounts.containsKey(temp)) keyCounts.put(temp, new HadoopCLMutableInteger());\n')
        secondLines.append('                               keyCounts.get(temp).incr();\n')
        
        writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLines, secondLines)
        
        fp.write('        return keyCounts;\n')
    fp.write('    }\n')

def writeInitFromMapperBufferMethod(fp, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType, hadoopInputKeyType, hadoopInputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void init(HadoopCLMapperBuffer mapperBuffer) {\n')
    fp.write('        this.keep = false;\n')
    fp.write('        this.tempBuffer1 = null;\n')
    fp.write('        this.clContext = mapperBuffer.clContext;\n')
    fp.write('        this.isGPU = this.clContext.isGPU();\n')
    fp.write('        this.maxInputValsPerInputKey = 0;\n')
    fp.write('\n')
    fp.write('        HashMap<'+hadoopInputKeyType+'Writable, HadoopCLMutableInteger> keyCounts = (HashMap<'+hadoopInputKeyType+'Writable, HadoopCLMutableInteger>)mapperBuffer.getKeyCounts();\n')
    fp.write('\n')
    fp.write('        this.nKeys = keyCounts.keySet().size();\n')
    fp.write('        this.inputKeys = new '+nativeInputKeyType+'[this.nKeys];\n')
    fp.write('        this.keyIndex = new int[this.nKeys];\n')
    fp.write('        this.nWrites = new int[this.nKeys];\n')
    fp.write('        this.outputsPerInput = this.clContext.getReducerKernel().getOutputPairsPerInput();\n')
    fp.write('        this.keyCapacity = this.nKeys;\n')
    fp.write('\n')
    fp.write('        int nTotalValues = 0;\n')
    fp.write('        int index = 0;\n')
    fp.write('        for('+hadoopInputKeyType+'Writable key : keyCounts.keySet()) {\n')
    fp.write('            this.keyIndex[index] = nTotalValues;\n')
    fp.write('            this.inputKeys[index] = key.get();\n')
    fp.write('            nTotalValues = nTotalValues + keyCounts.get(key).get();\n')
    fp.write('            index = index + 1;\n')
    fp.write('        }\n')
    fp.write('        this.inputVals = new '+nativeInputValueType+'[nTotalValues];\n')
    fp.write('        this.nVals = nTotalValues;\n')
    fp.write('        this.valCapacity = nTotalValues;\n')
    fp.write('\n')
    fp.write('        mapperBuffer.populate(this.inputKeys, this.inputVals, this.keyIndex);\n')
    fp.write('\n')
    fp.write('        if(this.outputsPerInput == -1) {\n')
    fp.write('            this.outputKeys = new '+nativeOutputKeyType+'[this.nVals];\n')
    fp.write('            this.outputVals = new '+nativeOutputValueType+'[this.nVals];\n')
    fp.write('        } else {\n')
    fp.write('            this.outputKeys = new '+nativeOutputKeyType+'[this.nKeys * this.outputsPerInput];\n')
    fp.write('            this.outputVals = new '+nativeOutputValueType+'[this.nKeys * this.outputsPerInput];\n')
    fp.write('        }\n')
    fp.write('    }\n')

def writeOriginalInitMethod(fp, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {\n')
    fp.write('        baseInit(clContext);\n')
    fp.write('        this.keep = true;\n')
    if nativeInputValueType == 'svec':
        fp.write('        this.individualInputValsCount = 0;\n')
    fp.write('        this.outputsPerInput = outputsPerInput;\n')
    fp.write('\n')
    if not isMapper:
        fp.write('        int inputValPerInputKey = this.getInputValPerInputKey();\n')
        if nativeInputValueType == 'pair':
            fp.write('        this.tempBuffer1 = new HadoopCLResizableDoubleArray();\n')
            fp.write('        this.tempBuffer2 = new HadoopCLResizableDoubleArray();\n')
        elif nativeInputValueType == 'ipair':
            fp.write('        this.tempBuffer1 = new HadoopCLResizableIntArray();\n')
            fp.write('        this.tempBuffer2 = new HadoopCLResizableDoubleArray();\n')
            fp.write('        this.tempBuffer3 = new HadoopCLResizableDoubleArray();\n')
        elif nativeInputValueType == 'svec':
            fp.write('        this.tempBuffer1 = new HadoopCLResizableIntArray();\n')
            fp.write('        this.tempBuffer2 = new HadoopCLResizableIntArray();\n')
            fp.write('        this.tempBuffer3 = new HadoopCLResizableDoubleArray();\n')
        else:
            fp.write('        this.tempBuffer1 = new HadoopCLResizable'+nativeInputValueType.capitalize()+'Array();\n')
        fp.write('\n')

    generateKeyValInit('inputKey', nativeInputKeyType, fp, 'this.clContext.getBufferSize()', False, False, True)

    if isMapper:
        generateKeyValInit('inputVal', nativeInputValueType, fp, 'this.clContext.getBufferSize()', False, False, True)
    else:
        generateKeyValInit('inputVal', nativeInputValueType, fp, 'this.clContext.getBufferSize() * inputValPerInputKey', False, False, True)

    if isMapper:
        generateKeyValInit('outputKey', nativeOutputKeyType, fp, 'this.clContext.getBufferSize() * outputsPerInput', False, False, False)
        generateKeyValInit('outputVal', nativeOutputValueType, fp, 'this.clContext.getBufferSize() * outputsPerInput', False, False, False)
        if nativeOutputValueType == 'svec':
            fp.write('        outputValLengthBuffer = new int[this.clContext.getBufferSize() * outputsPerInput];\n')
            fp.write('        memAuxIntIncr = new int[1];\n')
            fp.write('        memAuxDoubleIncr = new int[1];\n')
    else:
        fp.write('        if(outputsPerInput == -1) {\n')
        generateKeyValInit('outputKey', nativeOutputKeyType, fp, 'this.clContext.getBufferSize() * inputValPerInputKey', True, True, False)
        generateKeyValInit('outputVal', nativeOutputValueType, fp, 'this.clContext.getBufferSize() * inputValPerInputKey', True, True, False)
        fp.write('        } else {\n')
        generateKeyValInit('outputKey', nativeOutputKeyType, fp, 'this.clContext.getBufferSize() * outputsPerInput', True, False, False)
        generateKeyValInit('outputVal', nativeOutputValueType, fp, 'this.clContext.getBufferSize() * outputsPerInput', True, False, False)
        fp.write('        }\n')

    fp.write('    }\n')
    fp.write('\n')

def writeSetupParameter(fp, basename, type, isInput):
    if type == 'pair':
        fp.write('double[] set'+basename.capitalize()+'s1, ')
        fp.write('double[] set'+basename.capitalize()+'s2')
    elif type == 'ipair':
        fp.write('int[] set'+basename.capitalize()+'Ids, ')
        fp.write('double[] set'+basename.capitalize()+'s1, ')
        fp.write('double[] set'+basename.capitalize()+'s2')
    elif type == 'svec':
        if isInput:
            fp.write('int[] set'+basename.capitalize()+'LookAsideBuffer, ')
        else:
            fp.write('int[] set'+basename.capitalize()+'IntLookAsideBuffer, ')
            fp.write('int[] set'+basename.capitalize()+'DoubleLookAsideBuffer, ')
        fp.write('int[] set'+basename.capitalize()+'Indices, ')
        fp.write('double[] set'+basename.capitalize()+'Vals')
    else:
        fp.write(type+'[] set'+basename.capitalize()+'s')

def writeSetupDeclaration(fp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('    public void setup(')
    writeSetupParameter(fp, 'inputKey', nativeInputKeyType, True)
    fp.write(', ')
    writeSetupParameter(fp, 'inputVal', nativeInputValueType, True)
    fp.write(', ')
    writeSetupParameter(fp, 'outputKey', nativeOutputKeyType, False)
    fp.write(', ')
    writeSetupParameter(fp, 'outputVal', nativeOutputValueType, False)
    if nativeOutputValueType == 'svec':
        fp.write(', int[] setOutputValLengthBuffer')

    if isMapper:
        fp.write(', int[] setNWrites, int setNPairs')
    else:
        fp.write(', int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals')

    if nativeInputValueType == 'svec':
        fp.write(', int setIndividualInputValsCount')

    if nativeOutputValueType == 'svec':
        fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxDoubleIncr')
        #fp.write(', int[] setMemAuxIncr')
    fp.write(', int[] setMemIncr) {\n')

def writeSetupAndInitMethod(fp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    writeSetupDeclaration(fp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    generateKeyValSetup('inputKey', nativeInputKeyType, fp, True)
    generateKeyValSetup('inputVal', nativeInputValueType, fp, True)
    generateKeyValSetup('outputKey', nativeOutputKeyType, fp, False)
    generateKeyValSetup('outputVal', nativeOutputValueType, fp, False)

    if nativeOutputValueType == 'svec':
        fp.write('        this.outputValLengthBuffer = setOutputValLengthBuffer;\n')

    if isMapper:
        fp.write('        this.output_nWrites = setNWrites;\n')
        fp.write('        this.nPairs = setNPairs;\n')
    else:
        fp.write('        this.input_keyIndex = setKeyIndex;\n')
        fp.write('        this.output_nWrites = setNWrites;\n')
        fp.write('        this.nKeys = setNKeys;\n')
        fp.write('        this.nVals = setNVals;\n')

    fp.write('\n')
    fp.write('        this.memIncr = setMemIncr;\n')
    fp.write('        this.memIncr[0] = 0;\n')
    fp.write('\n')

    if nativeOutputValueType == 'pair':
        fp.write('        this.outputLength = outputVals1.length;\n')
    elif nativeOutputValueType == 'ipair':
        fp.write('        this.outputLength = outputValIds.length;\n')
    elif nativeOutputValueType == 'svec':
        fp.write('        this.outputLength = outputValIntLookAsideBuffer.length;\n')
        fp.write('        this.outputAuxLength = outputValIndices.length;\n')
    else:
        fp.write('        this.outputLength = outputVals.length;\n')


    if nativeInputValueType == 'svec':
        fp.write('        this.individualInputValsCount = setIndividualInputValsCount;\n')
        fp.write('\n')

    if nativeOutputValueType == 'svec':
        fp.write('        this.memAuxIntIncr = setMemAuxIntIncr;\n')
        fp.write('        this.memAuxDoubleIncr = setMemAuxDoubleIncr;\n')
        fp.write('        this.memAuxIntIncr[0] = 0;\n')
        fp.write('        this.memAuxDoubleIncr[0] = 0;\n')
        #fp.write('        this.memAuxIncr = setMemAuxIncr;\n')
        #fp.write('        this.memAuxIncr[0] = 0;\n')
        fp.write('\n')

    fp.write('    }\n')
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void init(HadoopOpenCLContext clContext) {\n')
    fp.write('        baseInit(clContext);\n')
    if isMapper and nativeInputValueType == 'svec':
        fp.write('        this.setStrided(this.clContext.runningOnGPU());\n')
    else:
        fp.write('        this.setStrided(false);\n')
    fp.write('    }\n')
    fp.write('\n')


def writeAddValueMethod(fp, hadoopInputValueType, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public void addTypedValue(Object val) {\n')
    fp.write('        '+hadoopInputValueType+'Writable actual = ('+hadoopInputValueType+'Writable)val;\n')
    if isMapper:
        if nativeInputValueType == 'pair':
            fp.write('        this.inputVals1[this.nPairs] = actual.getVal1();\n')
            fp.write('        this.inputVals2[this.nPairs] = actual.getVal2();\n')
        elif nativeInputValueType == 'ipair':
            fp.write('        this.inputValIds[this.nPairs] = actual.getIVal();\n')
            fp.write('        this.inputVals1[this.nPairs] = actual.getVal1();\n')
            fp.write('        this.inputVals2[this.nPairs] = actual.getVal2();\n')
        elif nativeInputValueType == 'svec':
            fp.write('        this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;\n')
            fp.write('        if (this.enableStriding) {\n')
            fp.write('            this.bufferValuesBeforeStriding.add(actual);\n')
            fp.write('        } else {\n')
            fp.write('            for(int i = 0; i < actual.size(); i++) {\n')
            fp.write('                this.inputValIndices[this.individualInputValsCount + i] = actual.indices()[i];\n')
            fp.write('                this.inputValVals[this.individualInputValsCount + i] = actual.vals()[i];\n')
            fp.write('            }\n')
            fp.write('        }\n')
            fp.write('        this.individualInputValsCount += actual.size();\n')
        else:
            fp.write('        this.inputVals[this.nPairs] = actual.get();\n')
    else:
        if nativeInputValueType == 'pair':
            fp.write('        this.inputVals1[this.nVals] = actual.getVal1();\n')
            fp.write('        this.inputVals2[this.nVals] = actual.getVal2();\n')
        elif nativeInputValueType == 'ipair':
            fp.write('        this.inputValIds[this.nVals] = actual.getIVal();\n')
            fp.write('        this.inputVals1[this.nVals] = actual.getVal1();\n')
            fp.write('        this.inputVals2[this.nVals] = actual.getVal2();\n')
        elif nativeInputValueType == 'svec':
            fp.write('        this.inputValLookAsideBuffer[this.nVals] = this.individualInputValsCount;\n')
            fp.write('        for(int i = 0 ; i < actual.size(); i++) {\n')
            fp.write('            this.inputValIndices[this.individualInputValsCount + i] = actual.indices()[i];\n')
            fp.write('            this.inputValVals[this.individualInputValsCount + i] = actual.vals()[i];\n')
            fp.write('        }\n')
            fp.write('        this.individualInputValsCount += actual.size();\n')
        else:
            fp.write('        this.inputVals[this.nVals] = actual.get();\n')

    fp.write('    }\n')
    fp.write('\n')

def writeAddKeyMethod(fp, hadoopInputKeyType, nativeInputKeyType):
    fp.write('    @Override\n')
    fp.write('    public void addTypedKey(Object key) {\n')
    fp.write('        '+hadoopInputKeyType+'Writable actual = ('+hadoopInputKeyType+'Writable)key;\n')
    if isMapper:
        if nativeInputKeyType == 'pair':
            fp.write('        this.inputKeys1[this.nPairs] = actual.getVal1();\n')
            fp.write('        this.inputKeys2[this.nPairs] = actual.getVal2();\n')
        elif nativeInputKeyType == 'ipair':
            fp.write('        this.inputKeyIds[this.nPairs] = actual.getIVal();\n')
            fp.write('        this.inputKeys1[this.nPairs] = actual.getVal1();\n')
            fp.write('        this.inputKeys2[this.nPairs] = actual.getVal2();\n')
        elif nativeInputKeyType == 'svec':
            print 'Unsupported key type svec'
            sys.exit(1)
        else:
            fp.write('        this.inputKeys[this.nPairs] = actual.get();\n')
    else:
        if nativeInputKeyType == 'pair':
            fp.write('        this.inputKeys1[this.nKeys] = actual.getVal1();\n')
            fp.write('        this.inputKeys2[this.nKeys] = actual.getVal2();\n')
        elif nativeInputKeyType == 'ipair':
            fp.write('        this.inputKeyIds[this.nKeys] = actual.getIVal();\n')
            fp.write('        this.inputKeys1[this.nKeys] = actual.getVal1();\n')
            fp.write('        this.inputKeys2[this.nKeys] = actual.getVal2();\n')
        elif nativeInputKeyType == 'svec':
            print 'Unsupported key type svec'
            sys.exit(1)
        else:
            fp.write('        this.inputKeys[this.nKeys] = actual.get();\n')

    fp.write('    }\n')
    fp.write('\n')

def writeTransferBufferedValues(fp, isMapper):
    fp.write('    @Override\n')
    fp.write('    public void transferBufferedValues(HadoopCLBuffer buffer) {\n')
    if isMapper:
        fp.write('        // NOOP\n')
    else:
        fp.write('        this.tempBuffer1.copyTo(((HadoopCLReducerBuffer)buffer).tempBuffer1);\n')
        fp.write('        if(this.tempBuffer2 != null) this.tempBuffer2.copyTo(((HadoopCLReducerBuffer)buffer).tempBuffer2);\n')
        fp.write('        if(this.tempBuffer3 != null) this.tempBuffer3.copyTo(((HadoopCLReducerBuffer)buffer).tempBuffer3);\n')
    fp.write('    }\n')

def writeResetMethod(fp, isMapper, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public void reset() {\n')
    fp.write('        this.resetProfile();\n')
    if isMapper:
        fp.write('        this.nPairs = 0;\n')
        if nativeInputValueType == 'svec':
            fp.write('        this.individualInputValsCount = 0;\n')
    else:
        fp.write('        this.nKeys = 0;\n')
        fp.write('        this.nVals = 0;\n')
        fp.write('        this.maxInputValsPerInputKey = 0;\n')

    fp.write('    }\n')
    fp.write('\n')

def writeIsFullMethod(fp, isMapper, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {\n')
    if isMapper:
        if nativeInputValueType == 'svec':
            fp.write('        SparseVectorWritable curr = (SparseVectorWritable)((Context)context).getCurrentValue();\n')
            fp.write('        if (this.enableStriding) {\n')
            fp.write('            int requiredValueCapacity = requiredCapacity(this.bufferValuesBeforeStriding, curr);\n')
            fp.write('            return this.nPairs == this.capacity() || requiredValueCapacity > this.inputValIndices.length;\n')
            fp.write('        } else {\n')
            fp.write('            return this.nPairs == this.capacity() || this.individualInputValsCount + curr.size() > this.inputValIndices.length;\n')
            fp.write('        }\n')
        else:
            fp.write('        return this.nPairs == this.capacity();\n')
    else:
        fp.write('        Context reduceContext = (Context)context;\n')
        fp.write('        tempBuffer1.reset();\n')
        fp.write('        if(tempBuffer2 != null) tempBuffer2.reset();\n')
        fp.write('        if(tempBuffer3 != null) tempBuffer3.reset();\n')
        fp.write('        for(Object v : reduceContext.getValues()) {\n')
        fp.write('            bufferInputValue(v);\n')
        fp.write('        }\n')
        fp.write('        return (this.nKeys == this.keyCapacity || this.nVals + this.numBufferedValues() > this.valCapacity);\n')

    fp.write('    }\n')
    fp.write('\n')
    
def writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLoopLines, secondLoopLines):
    fp.write('            if (this.memIncr[0] != 0) {\n')

    if nativeOutputKeyType == 'pair':
        fp.write('               int limit = this.outputKeys1.length < this.memIncr[0] ? this.outputKeys1.length : this.memIncr[0];\n')
    elif nativeOutputKeyType == 'ipair':
        fp.write('               int limit = this.outputKeyIds.length < this.memIncr[0] ? this.outputKeyIds.length : this.memIncr[0];\n')
    else:
        fp.write('               int limit = this.outputKeys.length < this.memIncr[0] ? this.outputKeys.length : this.memIncr[0];\n')

    fp.write('               for(int i = 0; i < limit; i++) {\n')

    for line in firstLoopLines:
        fp.write(line.replace('DUMMY', 'i'))

    fp.write('               }\n')
    fp.write('            } else {\n')
    fp.write('               if(isGPU == 0) {\n')
    if isMapper:
        fp.write('                   for(int i = 0; i < this.nPairs; i++) {\n')
    else:
        fp.write('                   for(int i = 0; i < this.nKeys; i++) {\n')
    fp.write('                       for(int j = 0; j < this.nWrites[i]; j++) {\n')

    for line in firstLoopLines:
        fp.write(line.replace('DUMMY', 'i * this.outputsPerInput + j'))

    fp.write('                       }\n')
    fp.write('                   }\n')
    fp.write('               } else {\n')
    fp.write('                   int j = 0;\n')
    fp.write('                   boolean someLeft = false;\n')
    fp.write('                   int base = 0;\n')
    fp.write('                   do {\n')
    fp.write('                       someLeft = false;\n')
    if isMapper:
        fp.write('                       for(int i = 0; i < this.nPairs; i++) {\n')
    else:
        fp.write('                       for(int i = 0; i < this.nKeys; i++) {\n')
    fp.write('                           if(this.nWrites[i] > j) {\n')
    for line in secondLoopLines:
        fp.write(line.replace('DUMMY', 'base + i'))

    fp.write('                               if(this.nWrites[i] > j + 1) someLeft = true;\n')
    fp.write('                           }\n')
    fp.write('                       }\n')

    if isMapper:
        fp.write('                       base += this.nPairs;\n')
    else:
        fp.write('                       base += this.nKeys;\n')

    fp.write('                       j++;\n')
    fp.write('                   } while(someLeft);\n')
    fp.write('               }\n')
    fp.write('            }\n')

def writeToHadoopMethod(fp, isMapper, hadoopOutputKeyType, hadoopOutputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public HadoopCLReducerBuffer putOutputsIntoHadoop(TaskInputOutputContext context, boolean doIntermediateReduction) throws IOException, InterruptedException {\n')
    fp.write('        final '+hadoopOutputKeyType+'Writable saveKey = new '+hadoopOutputKeyType+'Writable();\n')
    if nativeOutputValueType == 'svec':
        fp.write('        HadoopCLResizableIntArray indices = new HadoopCLResizableIntArray();\n')
        fp.write('        HadoopCLResizableDoubleArray vals = new HadoopCLResizableDoubleArray();\n')
        fp.write('        final '+hadoopOutputValueType+'Writable saveVal = new '+hadoopOutputValueType+'Writable(indices, vals);\n')
        fp.write('        int count;\n')
        fp.write('        if(this.memIncr[0] < 0 || this.outputValIntLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValIntLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
        fp.write('        for(int i = 0; i < count; i++) {\n')
        fp.write('            int intStartOffset = this.outputValIntLookAsideBuffer[i];\n')
        fp.write('            int doubleStartOffset = this.outputValDoubleLookAsideBuffer[i];\n')
        fp.write('            int length = this.outputValLengthBuffer[i];\n')
        fp.write('            indices.reset();\n')
        fp.write('            vals.reset();\n')
        fp.write('            for(int j = 0; j < length; j++) {\n')
        fp.write('                indices.add(this.outputValIndices[intStartOffset + j]);\n')
        fp.write('                vals.add(this.outputValVals[doubleStartOffset + j]);\n')
        fp.write('            }\n')
        fp.write(generateKeyValSet('Key', nativeOutputKeyType, 'i', ''))
        fp.write('            context.write(saveKey, saveVal);\n')
        fp.write('        }\n')
        fp.write('        return null;\n')
    else:
        fp.write('        final '+hadoopOutputValueType+'Writable saveVal = new '+hadoopOutputValueType+'Writable();\n')
        if isMapper and isPrimitiveHadoopType(hadoopOutputValueType):
            fp.write('        HadoopCLReducerBuffer reducerBuffer = null;\n')
            fp.write('        if(doIntermediateReduction) {\n')
            fp.write('            try {\n')
            fp.write('                reducerBuffer = (HadoopCLReducerBuffer)(clContext.getReducerKernel().getBufferClass().newInstance());\n')
            fp.write('            } catch(Exception ex) {\n')
            fp.write('                throw new RuntimeException(ex);\n')
            fp.write('            }\n')
            fp.write('            reducerBuffer.init(this);\n')
            
            fp.write('        } else {\n')

        firstLines = [ ]
        firstLines.append(generateKeyValSet('Key', nativeOutputKeyType, 'DUMMY', '        '))
        firstLines.append(generateKeyValSet('Val', nativeOutputValueType, 'DUMMY', '        '))
        firstLines.append('                            context.write(saveKey, saveVal);\n')

        secondLines = [ ]
        secondLines.append(generateKeyValSet('Key', nativeOutputKeyType, 'DUMMY', '                '))
        secondLines.append(generateKeyValSet('Val', nativeOutputValueType, 'DUMMY', '                '))
        secondLines.append('                                    context.write(saveKey, saveVal);\n')

        writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLines, secondLines)

        if isMapper and isPrimitiveHadoopType(hadoopOutputValueType):
            fp.write('        }\n')
            fp.write('        return reducerBuffer;\n')
        else:
            fp.write('        return null;\n')
    fp.write('    }\n')

def generateCloneIncompleteMethod(fp, isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType):
    fp.write('    @Override\n')
    fp.write('    public HadoopCLBuffer cloneIncomplete() {\n')
    fullClassName = bufferClassName(isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType)
    fp.write('        '+fullClassName+' newBuffer = null;\n')
    fp.write('        try {\n')
    fp.write('            newBuffer = ('+fullClassName+')this.getClass().newInstance();\n')
    fp.write('        } catch(Exception ex) {\n')
    fp.write('            throw new RuntimeException(ex);\n')
    fp.write('        }\n')
    fp.write('        newBuffer.init(this.outputsPerInput, this.clContext);\n')
    if isMapper:
        fp.write('        for(int i = 0; i < this.nPairs; i++) {\n')
    else:
        fp.write('        for(int i = 0; i < this.nKeys; i++) {\n')

    fp.write('            if(this.nWrites[i] == -1) {\n')
    if isMapper:
        if nativeInputKeyType == 'pair':
            fp.write('            newBuffer.inputKeys1[newBuffer.nPairs] = this.inputKeys1[i];\n')
            fp.write('            newBuffer.inputKeys2[newBuffer.nPairs] = this.inputKeys2[i];\n')
        elif nativeInputKeyType == 'ipair':
            fp.write('            newBuffer.inputKeyIds[newBuffer.nPairs] = this.inputKeyIds[i];\n')
            fp.write('            newBuffer.inputKeys1[newBuffer.nPairs] = this.inputKeys1[i];\n')
            fp.write('            newBuffer.inputKeys2[newBuffer.nPairs] = this.inputKeys2[i];\n')
        elif nativeInputKeyType == 'svec':
            print 'Unsupported input key type svec'
            sys.exit(1)
        else:
            fp.write('            newBuffer.inputKeys[newBuffer.nPairs] = this.inputKeys[i];\n')

        if nativeInputValType == 'pair':
            fp.write('            newBuffer.inputVals1[newBuffer.nPairs] = this.inputVals1[i];\n')
            fp.write('            newBuffer.inputVals2[newBuffer.nPairs] = this.inputVals2[i];\n')
        elif nativeInputValType == 'ipair':
            fp.write('            newBuffer.inputValIds[newBuffer.nPairs] = this.inputValIds[i];\n')
            fp.write('            newBuffer.inputVals1[newBuffer.nPairs] = this.inputVals1[i];\n')
            fp.write('            newBuffer.inputVals2[newBuffer.nPairs] = this.inputVals2[i];\n')
        elif nativeInputValType == 'svec':
            fp.write('            newBuffer.inputValLookAsideBuffer[newBuffer.nPairs] = newBuffer.individualInputValsCount;\n')
            fp.write('            int baseOffset = this.inputValLookAsideBuffer[i];\n')
            fp.write('            int topOffset = i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1];\n')
            fp.write('            System.arraycopy(this.inputValIndices, baseOffset, newBuffer.inputValIndices, newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);\n')
            fp.write('            System.arraycopy(this.inputValVals, baseOffset, newBuffer.inputValVals, newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);\n')
            fp.write('            newBuffer.individualInputValsCount += (topOffset - baseOffset);\n')
        else:
            fp.write('            newBuffer.inputVals[newBuffer.nPairs] = this.inputVals[i];\n')
        fp.write('            newBuffer.nPairs++;\n')

    else:
        if nativeInputKeyType == 'pair':
            fp.write('            newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];\n')
            fp.write('            newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];\n')
        elif nativeInputKeyType == 'ipair':
            fp.write('            newBuffer.inputKeyIds[newBuffer.nKeys] = this.inputKeyIds[i];\n')
            fp.write('            newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];\n')
            fp.write('            newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];\n')
        elif nativeInputKeyType == 'svec':
            print 'Unsupported input key type svec on reducer'
            sys.exit(1)
        else:
            fp.write('            newBuffer.inputKeys[newBuffer.nKeys] = this.inputKeys[i];\n')
        fp.write('            newBuffer.keyIndex[newBuffer.nKeys] = newBuffer.nVals;\n')
        fp.write('            int baseValOffset = this.keyIndex[i];\n')
        fp.write('            int topValOffset = (i == this.nKeys-1 ? this.nVals : this.keyIndex[i+1]);\n')
        if nativeInputValType == 'pair':
            fp.write('            System.arraycopy(this.inputVals1, baseValOffset, newBuffer.inputVals1, newBuffer.nVals, topValOffset - baseValOffset);\n')
            fp.write('            System.arraycopy(this.inputVals2, baseValOffset, newBuffer.inputVals2, newBuffer.nVals, topValOffset - baseValOffset);\n')
        elif nativeInputValType == 'ipair':
            fp.write('            System.arraycopy(this.inputValIds, baseValOffset, newBuffer.inputValIds, newBuffer.nVals, topValOffset - baseValOffset);\n')
            fp.write('            System.arraycopy(this.inputVals1, baseValOffset, newBuffer.inputVals1, newBuffer.nVals, topValOffset - baseValOffset);\n')
            fp.write('            System.arraycopy(this.inputVals2, baseValOffset, newBuffer.inputVals2, newBuffer.nVals, topValOffset - baseValOffset);\n')
        elif nativeInputValType == 'svec':
            fp.write('            for(int j = baseValOffset; j < topValOffset; j++) {\n')
            fp.write('                int offsetInNewBuffer = newBuffer.nVals + j-baseValOffset;\n')
            fp.write('                newBuffer.inputValLookAsideBuffer[offsetInNewBuffer] = newBuffer.individualInputValsCount;\n')
            fp.write('                int baseOffset = this.inputValLookAsideBuffer[j];\n')
            fp.write('                int topOffset = j == this.nVals-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[j+1];\n')
            fp.write('                System.arraycopy(this.inputValIndices, baseOffset, newBuffer.inputValIndices, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);\n')
            fp.write('                System.arraycopy(this.inputValVals, baseOffset, newBuffer.inputValVals, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);\n')
            fp.write('                newBuffer.individualInputValsCount += (topOffset - baseOffset);\n')
            fp.write('            }\n')
        else:
            fp.write('            System.arraycopy(this.inputVals, baseValOffset, newBuffer.inputVals, newBuffer.nVals, topValOffset - baseValOffset);\n')
        fp.write('            newBuffer.nVals += (topValOffset - baseValOffset);\n')
        fp.write('            newBuffer.nKeys++;\n')
    fp.write('            }\n')
    fp.write('        }\n')
    fp.write('        return newBuffer;\n')
    fp.write('    }\n\n')

def writeFillParameter(fp, basename, type, isInput):
    if type == 'pair':
        fp.write('this.'+basename+'s1, this.'+basename+'s2')
    elif type == 'ipair':
        fp.write('this.'+basename+'Ids, this.'+basename+'s1, this.'+basename+'s2')
    elif type == 'svec':
        if isInput:
            fp.write('this.'+basename+'LookAsideBuffer, this.'+basename+'Indices, this.'+basename+'Vals')
        else:
            fp.write('this.'+basename+'IntLookAsideBuffer, this.'+basename+'DoubleLookAsideBuffer, this.'+basename+'Indices, this.'+basename+'Vals')
    else:
        fp.write('this.'+basename+'s')

def generateFill(fp, isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType):
    kernelClass = kernelClassName(isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType)
    fp.write('    @Override\n')
    fp.write('    public void fill(HadoopCLKernel generalKernel) {\n')
    fp.write('        '+kernelClass+' kernel = ('+kernelClass+')generalKernel;\n')

    if nativeInputValType == 'svec' and isMapper:
        fp.write('        if (this.enableStriding) {\n')
        fp.write('            int valueIndex = 0;\n')
        fp.write('            int nBufferedValues = this.bufferValuesBeforeStriding.size();\n')
        fp.write('            for (SparseVectorWritable v : this.bufferValuesBeforeStriding) {\n')
        fp.write('                int length = v.size();\n')
        fp.write('                int[] indices = v.indices();\n')
        fp.write('                double[] vals = v.vals();\n')
        fp.write('                for (int i = 0; i < length; i++) {\n')
        fp.write('                    this.inputValIndices[valueIndex + (i * nBufferedValues)] = indices[i];\n')
        fp.write('                    this.inputValVals[valueIndex + (i * nBufferedValues)] = vals[i];\n')
        fp.write('                }\n')
        fp.write('                valueIndex++;\n')
        fp.write('            }\n')
        fp.write('            this.bufferValuesBeforeStriding.clear();\n')
        fp.write('        }\n')

    if not isMapper:
        fp.write('        if(this.outputsPerInput == -1 && (this.outputKeys == null || this.outputKeys.length < this.nKeys * this.maxInputValsPerInputKey)) {\n')
        generateKeyValInit('outputKey', nativeOutputKeyType, fp, 'this.nKeys * this.maxInputValsPerInputKey', True, False, False)
        generateKeyValInit('outputVal', nativeOutputValType, fp, 'this.nKeys * this.maxInputValsPerInputKey', True, False, False)
        fp.write('        }\n')
    fp.write('        kernel.setup(')
    writeFillParameter(fp, 'inputKey', nativeInputKeyType, True)
    fp.write(', ')
    writeFillParameter(fp, 'inputVal', nativeInputValType, True)
    fp.write(', ')
    writeFillParameter(fp, 'outputKey', nativeOutputKeyType, False)
    fp.write(', ')
    writeFillParameter(fp, 'outputVal', nativeOutputValType, False)
    if nativeOutputValType == 'svec':
        fp.write(', outputValLengthBuffer')
    if isMapper:
        fp.write(', nWrites, nPairs')
    else:
        fp.write(', keyIndex, nWrites, nKeys, nVals')

    if nativeInputValType == 'svec':
        fp.write(', individualInputValsCount')

    if nativeOutputValType == 'svec':
        fp.write(', this.memAuxIntIncr, this.memAuxDoubleIncr')
        #fp.write(', this.memAuxIncr')

    fp.write(', this.memIncr);\n')
    fp.write('    }\n')
    fp.write('\n')

def generateMapArguments(varName, nativeType):
    if nativeType == 'pair':
        return varName+'.getVal1(), '+varName+'.getVal2()'
    elif nativeType == 'ipair':
        return varName+'.getIVal(), '+varName+'.getVal1(), '+varName+'.getVal2()'
    elif nativeType == 'svec':
        return varName+'.indices(), '+varName+'.vals(), '+varName+'.size()'
    else:
        return varName+'.get()'

def generateFile(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType):
    nativeInputKeyType = inputKeyType
    nativeInputValueType = inputValueType
    nativeOutputKeyType = outputKeyType
    nativeOutputValueType = outputValueType

    hadoopInputKeyType = typeNameForWritable(inputKeyType)
    hadoopInputValueType = typeNameForWritable(inputValueType)
    hadoopOutputKeyType = typeNameForWritable(outputKeyType)
    hadoopOutputValueType = typeNameForWritable(outputValueType)

    bufferfp = open('mapred/org/apache/hadoop/mapreduce/'+
        bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+'.java', 'w')

    kernelfp = open('mapred/org/apache/hadoop/mapreduce/'+
        kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+'.java', 'w')

    writeHeader(bufferfp, isMapper);
    writeHeader(kernelfp, isMapper);

    bufferfp.write('public class '+
        bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+' extends HadoopCL'+
        capitalizedKernelString(isMapper)+'Buffer {\n')

    kernelfp.write('public abstract class '+
        kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+' extends HadoopCL'+
        capitalizedKernelString(isMapper)+'Kernel {\n')

    generateKeyValDecl('inputKey', nativeInputKeyType, bufferfp, True)
    generateKeyValDecl('inputVal', nativeInputValueType, bufferfp, True)
    generateKeyValDecl('outputKey', nativeOutputKeyType, bufferfp, False)
    generateKeyValDecl('outputVal', nativeOutputValueType, bufferfp, False)
    if nativeOutputValueType == 'svec':
        bufferfp.write('    public int[] outputValLengthBuffer;\n')
    bufferfp.write('    protected int outputsPerInput;\n')
    kernelfp.write('    protected int outputLength;\n')

    if not isMapper:
        if nativeInputValueType == 'pair':
            kernelfp.write('    protected HadoopCLResizableDoubleArray bufferedVal1 = null;\n')
            kernelfp.write('    protected HadoopCLResizableDoubleArray bufferedVal2 = null;\n')
        elif nativeInputValueType == 'ipair':
            kernelfp.write('    protected HadoopCLResizableIntArray bufferedValId = null;\n')
            kernelfp.write('    protected HadoopCLResizableDoubleArray bufferedVal1 = null;\n')
            kernelfp.write('    protected HadoopCLResizableDoubleArray bufferedVal2 = null;\n')
        elif nativeInputValueType == 'svec':
            # kernelfp.write('    protected HadoopCLResizableIntArray bufferedValLookAside = null;\n')
            # kernelfp.write('    protected HadoopCLResizableIntArray bufferedValIndices = null;\n')
            # kernelfp.write('    protected HadoopCLResizableDoubleArray bufferedValVals = null;\n')
            pass
        else:
            kernelfp.write('    protected HadoopCLResizable'+nativeInputValueType.capitalize()+'Array bufferedVals = null;\n')

    kernelfp.write('\n')
    if nativeInputValueType == 'svec':
        bufferfp.write('    protected int individualInputValsCount;\n')
        kernelfp.write('    protected int individualInputValsCount;\n')
        if isMapper:
            kernelfp.write('    protected int currentInputVectorLength = -1;\n')
        bufferfp.write('    protected int[] memAuxIntIncr;\n')
        bufferfp.write('    protected int[] memAuxDoubleIncr;\n')
        kernelfp.write('    protected int[] memAuxIntIncr;\n')
        kernelfp.write('    protected int[] memAuxDoubleIncr;\n')
        kernelfp.write('    protected int outputAuxLength;\n')
        if isMapper: # unused if not enabled striding
            bufferfp.write('   protected List<SparseVectorWritable> bufferValuesBeforeStriding =\n')
            bufferfp.write('      new ArrayList<SparseVectorWritable>();\n')

    kernelfp.write('\n')
    bufferfp.write('\n')

    generateKeyValDecl('inputKey', nativeInputKeyType, kernelfp, True)
    generateKeyValDecl('inputVal', nativeInputValueType, kernelfp, True)
    generateKeyValDecl('outputKey', nativeOutputKeyType, kernelfp, False)
    generateKeyValDecl('outputVal', nativeOutputValueType, kernelfp, False)
    if nativeOutputValueType == 'svec':
        #kernelfp.write('    private HadoopCLResizableIntArray bufferOutputIndices = new HadoopCLResizableIntArray();\n')
        #kernelfp.write('    private HadoopCLResizableDoubleArray bufferOutputVals = new HadoopCLResizableDoubleArray();\n')
        kernelfp.write('    private int[] bufferOutputIndices = null;\n')
        kernelfp.write('    private double[] bufferOutputVals = null;\n')
        kernelfp.write('    public int[] outputValLengthBuffer;\n')
    kernelfp.write('    final private '+hadoopOutputKeyType+'Writable keyObj = new '+hadoopOutputKeyType+'Writable();\n')
    kernelfp.write('    final private '+hadoopOutputValueType+'Writable valObj = new '+hadoopOutputValueType+'Writable();\n')
    kernelfp.write('\n')
    generateKernelDecl(isMapper, nativeInputKeyType, nativeInputValueType, kernelfp)

    writeSetupAndInitMethod(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    kernelfp.write('    public Class getBufferClass() { return '+bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+'.class; }\n\n')

    if nativeOutputValueType == 'svec':
        kernelfp.write('    protected int[] allocInt(int len) {\n')
        kernelfp.write('        return new int[len];\n')
        kernelfp.write('    }\n')
        kernelfp.write('    protected double[] allocDouble(int len) {\n')
        kernelfp.write('        return new double[len];\n')
        kernelfp.write('    }\n')
        #kernelfp.write('    protected boolean reserveOutput(int len) {\n')
        #kernelfp.write('        this.bufferOutputIndices = new int[len];\n')
        #kernelfp.write('        this.bufferOutputVals = new double[len];\n')
        #kernelfp.write('        return true;\n')
        #kernelfp.write('    }\n')
        kernelfp.write('\n')
        #kernelfp.write('    protected void accessOutput(int ind, double val, int index) {\n')
        #kernelfp.write('        this.bufferOutputIndices[index] = ind;\n')
        #kernelfp.write('        this.bufferOutputVals[index] = val;\n')
        #kernelfp.write('    }\n')
        kernelfp.write('\n')
    else:
        #kernelfp.write('    protected boolean reserveOutput() {\n')
        #kernelfp.write('        return true;\n')
        #kernelfp.write('    }\n')
        kernelfp.write('\n')

    if nativeInputValueType == 'svec':
        if isMapper:
            kernelfp.write('    protected int inputVectorLength(int vid) {\n')
            kernelfp.write('       return this.currentInputVectorLength;\n')
            kernelfp.write('    }\n')
            kernelfp.write('\n')
        else:
            kernelfp.write('    protected int inputVectorLength(int vid) {\n')
            kernelfp.write('       return 0;\n')
            # kernelfp.write('       int start = ((int[])this.bufferedValLookAside.getArray())[vid];\n')
            # kernelfp.write('       int end = vid == this.bufferedValLookAside.size()-1 ? this.bufferedValIndices.size() : ((int[])this.bufferedValLookAside.getArray())[vid+1];\n')
            # kernelfp.write('       return end-start;\n')
            kernelfp.write('    }\n')

    if isMapper:
        writeKeyCountsMethod(bufferfp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType)
        writePopulatesMethod(bufferfp, nativeOutputKeyType, nativeOutputValueType)
    else:
        if isPrimitiveHadoopType(hadoopInputValueType):
            writeInitFromMapperBufferMethod(bufferfp, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType, hadoopInputKeyType, hadoopInputValueType)
        else:
             bufferfp.write('\n')
             bufferfp.write('    @Override\n')
             bufferfp.write('    public void init(HadoopCLMapperBuffer mapperBuffer) {\n')
             bufferfp.write('        throw new RuntimeException("Invalid to call this init method for complex input values");\n')
             bufferfp.write('    }\n')

    writeOriginalInitMethod(bufferfp, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)
    bufferfp.write('    @Override\n')
    bufferfp.write('    public Class getKernelClass() {\n')
    if isMapper:
        bufferfp.write('        return '+compactHadoopName(hadoopInputKeyType)+compactHadoopName(hadoopInputValueType)+compactHadoopName(hadoopOutputKeyType)+compactHadoopName(hadoopOutputValueType)+'HadoopCLMapperKernel.class;\n')
    else:
        bufferfp.write('        return '+compactHadoopName(hadoopInputKeyType)+compactHadoopName(hadoopInputValueType)+compactHadoopName(hadoopOutputKeyType)+compactHadoopName(hadoopOutputValueType)+'HadoopCLReducerKernel.class;\n')
    bufferfp.write('    }\n')

    generateFill(bufferfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    if not isMapper:
        bufferfp.write('    @Override\n')
        bufferfp.write('    public void bufferInputValue(Object obj) {\n')
        bufferfp.write('        '+hadoopInputValueType+'Writable actual = ('+hadoopInputValueType+'Writable)obj;\n')
        if nativeInputValueType == 'pair':
            bufferfp.write('        ((HadoopCLResizableDoubleArray)this.tempBuffer1).add(actual.getVal1());\n')
            bufferfp.write('        ((HadoopCLResizableDoubleArray)this.tempBuffer2).add(actual.getVal2());\n')
        elif nativeInputValueType == 'ipair':
            bufferfp.write('        ((HadoopCLResizableIntArray)this.tempBuffer1).add(actual.getIVal());\n')
            bufferfp.write('        ((HadoopCLResizableDoubleArray)this.tempBuffer2).add(actual.getVal1());\n')
            bufferfp.write('        ((HadoopCLResizableDoubleArray)this.tempBuffer3).add(actual.getVal2());\n')
        elif nativeInputValueType == 'svec':
            bufferfp.write('                ((HadoopCLResizableIntArray)this.tempBuffer1).add(this.tempBuffer2.size());\n')
            bufferfp.write('                for(int i = 0; i < actual.size(); i++) {\n')
            bufferfp.write('                    ((HadoopCLResizableIntArray)this.tempBuffer2).add(actual.indices()[i]);\n')
            bufferfp.write('                    ((HadoopCLResizableDoubleArray)this.tempBuffer3).add(actual.vals()[i]);\n')
            bufferfp.write('                }\n')
        else:
            bufferfp.write('        ((HadoopCLResizable'+nativeInputValueType.capitalize()+'Array)this.tempBuffer1).add(actual.get());\n')

        bufferfp.write('    }\n')
        bufferfp.write('\n')
        bufferfp.write('    @Override\n')
        bufferfp.write('    public void useBufferedValues() {\n')
        if nativeInputValueType == 'pair':
            bufferfp.write('        System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputVals1, this.nVals, this.tempBuffer1.size());\n')
            bufferfp.write('        System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVals2, this.nVals, this.tempBuffer2.size());\n')
        elif nativeInputValueType == 'ipair':
            bufferfp.write('        System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputValIds, this.nVals, this.tempBuffer1.size());\n')
            bufferfp.write('        System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVals1, this.nVals, this.tempBuffer2.size());\n')
            bufferfp.write('        System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputVals2, this.nVals, this.tempBuffer3.size());\n')
        elif nativeInputValueType == 'svec':
            bufferfp.write('        for(int i = 0; i < this.tempBuffer1.size(); i++) {\n')
            bufferfp.write('            this.inputValLookAsideBuffer[this.nVals + i] = this.individualInputValsCount + ((int[])this.tempBuffer1.getArray())[i];\n')
            bufferfp.write('        }\n')
            bufferfp.write('        System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputValIndices, this.individualInputValsCount, this.tempBuffer2.size());\n')
            bufferfp.write('        System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputValVals, this.individualInputValsCount, this.tempBuffer3.size());\n')
            bufferfp.write('        this.individualInputValsCount += this.tempBuffer2.size();\n')
        else:
            bufferfp.write('        System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputVals, this.nVals, this.tempBuffer1.size());\n')
        bufferfp.write('        this.nVals += this.tempBuffer1.size();\n')
        bufferfp.write('    }\n')

    writeAddValueMethod(bufferfp, hadoopInputValueType, nativeInputValueType)

    writeAddKeyMethod(bufferfp, hadoopInputKeyType, nativeInputKeyType)

    writeIsFullMethod(bufferfp, isMapper, nativeInputValueType)
    writeResetMethod(bufferfp, isMapper, nativeInputValueType)

    writeTransferBufferedValues(bufferfp, isMapper)

    writeToHadoopMethod(bufferfp, isMapper, hadoopOutputKeyType, hadoopOutputValueType, nativeOutputKeyType, nativeOutputValueType)

    bufferfp.write('\n')
    bufferfp.write('    @Override\n')
    bufferfp.write('    public boolean equalInputOutputTypes() {\n')
    if nativeInputKeyType == nativeOutputKeyType and nativeInputValueType == nativeOutputValueType:
        bufferfp.write('        return true;\n')
    else:
        bufferfp.write('        return false;\n')
    bufferfp.write('    }\n')

    generateCloneIncompleteMethod(bufferfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    generateWriteMethod(kernelfp, nativeOutputKeyType, nativeOutputValueType)

    generateKernelCall(isMapper, nativeInputKeyType, nativeInputValueType, kernelfp)

    kernelfp.write('    @Override\n')
    kernelfp.write('    public HadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {\n')
    kernelfp.write('        Context ctx = (Context)context;\n')
    kernelfp.write('        this.javaProfile = new HadoopCLAccumulatedProfile();\n')
    kernelfp.write('        this.javaProfile.startOverall();\n')
    if not isMapper:
        if nativeInputValueType == 'pair':
            kernelfp.write('        this.bufferedVal1 = new HadoopCLResizableDoubleArray();\n')
            kernelfp.write('        this.bufferedVal2 = new HadoopCLResizableDoubleArray();\n')
        elif nativeInputValueType == 'ipair':
            kernelfp.write('        this.bufferedValId = new HadoopCLResizableIntArray();\n')
            kernelfp.write('        this.bufferedVal1 = new HadoopCLResizableDoubleArray();\n')
            kernelfp.write('        this.bufferedVal2 = new HadoopCLResizableDoubleArray();\n')
        elif nativeInputValueType == 'svec':
            # kernelfp.write('        this.bufferedValLookAside = new HadoopCLResizableIntArray();\n')
            # kernelfp.write('        this.bufferedValIndices = new HadoopCLResizableIntArray();\n')
            # kernelfp.write('        this.bufferedValVals = new HadoopCLResizableDoubleArray();\n')
            pass
        else:
            kernelfp.write('        this.bufferedVals = new HadoopCLResizable'+nativeInputValueType.capitalize()+'Array();\n')

    kernelfp.write('        while(ctx.nextKeyValue()) {\n')
    kernelfp.write('            this.javaProfile.startRead();\n')
    if isMapper:
        kernelfp.write('            '+hadoopInputKeyType +'Writable key = ('+hadoopInputKeyType+'Writable)ctx.getCurrentKey();\n')
        kernelfp.write('            '+hadoopInputValueType +'Writable val = ('+hadoopInputValueType+'Writable)ctx.getCurrentValue();\n')
        if nativeInputValueType == 'svec':
            kernelfp.write('            this.currentInputVectorLength = val.size();\n')
        kernelfp.write('            this.javaProfile.stopRead();\n')
        kernelfp.write('            this.javaProfile.startKernel();\n')
        kernelfp.write('            map('+generateMapArguments('key', nativeInputKeyType)+', '+generateMapArguments('val', nativeInputValueType)+');\n')
        kernelfp.write('            this.javaProfile.stopKernel();\n')
    else:
        kernelfp.write('            '+hadoopInputKeyType +'Writable key = ('+hadoopInputKeyType+'Writable)ctx.getCurrentKey();\n')
        kernelfp.write('            Iterable<'+hadoopInputValueType+'Writable> values = (Iterable<'+hadoopInputValueType+'Writable>)ctx.getValues();\n')
        if nativeInputValueType == 'pair':
            kernelfp.write('            this.bufferedVal1.reset();\n')
            kernelfp.write('            this.bufferedVal2.reset();\n')
        elif nativeInputValueType == 'ipair':
            kernelfp.write('            this.bufferedValId.reset();\n')
            kernelfp.write('            this.bufferedVal1.reset();\n')
            kernelfp.write('            this.bufferedVal2.reset();\n')
        elif nativeInputValueType == 'svec':
            # kernelfp.write('            this.bufferedValLookAside.reset();\n')
            # kernelfp.write('            this.bufferedValIndices.reset();\n')
            # kernelfp.write('            this.bufferedValVals.reset();\n')
            kernelfp.write('            List<HadoopCLResizableIntArray> accIndices = new ArrayList<HadoopCLResizableIntArray>();\n')
            kernelfp.write('            List<HadoopCLResizableDoubleArray> accVals = new ArrayList<HadoopCLResizableDoubleArray>();\n')
        else:
            kernelfp.write('            this.bufferedVals.reset();\n')

        kernelfp.write('            for('+hadoopInputValueType+'Writable v : values) {\n')
        if nativeInputValueType == 'pair':
            kernelfp.write('                this.bufferedVal1.add(v.getVal1());\n')
            kernelfp.write('                this.bufferedVal2.add(v.getVal2());\n')
        elif nativeInputValueType == 'ipair':
            kernelfp.write('                this.bufferedValId.add(v.getIVal());\n')
            kernelfp.write('                this.bufferedVal1.add(v.getVal1());\n')
            kernelfp.write('                this.bufferedVal2.add(v.getVal2());\n')
        elif nativeInputValueType == 'svec':
            # kernelfp.write('                this.bufferedValLookAside.add(this.bufferedValIndices.size());\n')
            kernelfp.write('                HadoopCLResizableIntArray indices = new HadoopCLResizableIntArray();\n')
            kernelfp.write('                HadoopCLResizableDoubleArray vals = new HadoopCLResizableDoubleArray();\n')
            kernelfp.write('                for(int i = 0; i < v.size(); i++) {\n')
            kernelfp.write('                    indices.add(v.indices()[i]);\n')
            kernelfp.write('                    vals.add(v.vals()[i]);\n')
            # kernelfp.write('                    this.bufferedValIndices.add(v.indices()[i]);\n')
            # kernelfp.write('                    this.bufferedValVals.add(v.vals()[i]);\n')
            kernelfp.write('                }\n')
            kernelfp.write('                accIndices.add(indices);\n')
            kernelfp.write('                accVals.add(vals);\n')
        else:
            kernelfp.write('                this.bufferedVals.add(v.get());\n')

        kernelfp.write('            }\n')
        kernelfp.write('            this.javaProfile.stopRead();\n')
        kernelfp.write('            this.javaProfile.startKernel();\n')
        kernelfp.write('            reduce('+generateMapArguments('key',nativeInputKeyType)+', ')
        if nativeInputValueType == 'pair':
            kernelfp.write("""new HadoopCLPairValueIterator(
                           (double[])this.bufferedVal1.getArray(),
                           (double[])this.bufferedVal2.getArray(),
                           this.bufferedVal1.size()));\n""")
        elif nativeInputValueType == 'ipair':
            kernelfp.write("""new HadoopCLUPairValueIterator(
                           (int[])this.bufferedValId.getArray(),
                           (double[])this.bufferedVal1.getArray(),
                           (double[])this.bufferedVal2.getArray(),
                           this.bufferedValId.size()));\n""")
        elif nativeInputValueType == 'svec':
            kernelfp.write("""new HadoopCLSvecValueIterator(
                           accIndices, accVals));\n""")
            # kernelfp.write("""new HadoopCLSvecValueIterator(
            #                (int[])this.bufferedValLookAside.getArray(),
            #                (int[])this.bufferedValIndices.getArray(),
            #                (double[])this.bufferedValVals.getArray(),
            #                this.bufferedValLookAside.size(),
            #                this.bufferedValIndices.size()));\n""")
        else:
            kernelfp.write('new HadoopCL'+nativeInputValueType.capitalize()+
                    'ValueIterator(('+nativeInputValueType+
                    '[])this.bufferedVals.getArray(), this.bufferedVals.size()));\n')
        kernelfp.write('            this.javaProfile.stopKernel();\n')

    kernelfp.write('            OpenCLDriver.inputsRead++;\n')
    kernelfp.write('        }\n')
    kernelfp.write('        this.javaProfile.stopOverall();\n')
    kernelfp.write('        return this.javaProfile;\n')
    kernelfp.write('    }\n')

    bufferfp.write('}\n\n')
    kernelfp.write('}\n\n')

    bufferfp.close()
    kernelfp.close()

if(len(sys.argv) != 6):
    print 'usage: python AutoGenerateKernel.py mapper|reducer inputKeyType inputValueType outputKeyType outputValueType'
    print '    valid types include '+str(supportedTypes)
    sys.exit(-1)

inputKeyType = sys.argv[2]
inputValueType = sys.argv[3]
outputKeyType = sys.argv[4]
outputValueType = sys.argv[5]
kernelType = sys.argv[1]

if kernelType == 'mapper' or kernelType == 'm':
    isMapper = True
elif kernelType == 'reducer' or kernelType == 'r':
    isMapper = False
else:
    print 'Invalid kernel type specified: '+kernelType
    sys.exit(-1)

if not checkTypeSupported(inputKeyType):
    print 'Unsupported input key type '+inputKeyType
    sys.exit(-1)

if not checkTypeSupported(inputValueType):
    print 'Unsupported input value type '+inputValueType
    sys.exit(-1)

if not checkTypeSupported(outputKeyType):
    print 'Unsupported output key type '+outputKeyType
    sys.exit(-1)

if not checkTypeSupported(outputValueType):
    print 'Unsupported output value type '+outputValueType
    sys.exit(-1)

generateFile(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)
