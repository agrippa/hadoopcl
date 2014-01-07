import sys

primitives = [ 'int', 'float', 'double', 'long' ]
nonprimitives = [ 'pair', 'ipair' ]
variablelength = [ 'bsvec', 'svec', 'ivec', 'fsvec' ]
supportedTypes = primitives + nonprimitives + variablelength
profileMemoryUtilization = False

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


def writeln(arr, indent, fp):
    indent_str = '    ' * indent
    for token in arr:
        fp.write(indent_str+token+'\n')

def write_without_last_ln(arr, indent, fp):
    indent_str = '    ' * indent
    for token in arr[:len(arr)-1]:
        fp.write(indent_str+token+'\n')
    fp.write(indent_str+arr[len(arr)-1])

def write(arr, indent, fp):
    indent_str = '    ' * indent
    fp.write(indent_str+(''.join(arr)))

def tostr(arr, indent):
    buf = [ ]
    indent_str = '    ' * indent
    for token in arr:
        buf.append(indent_str+token+'\n')
    return ''.join(buf)

def tostr_without_last_ln(arr, indent):
    buf = [ ]
    indent_str = '    ' * indent
    for token in arr[:len(arr)-1]:
        buf.append(indent_str+token+'\n')
    buf.append(indent_str+arr[len(arr)-1])
    return ''.join(buf)


#################################################################################
########################## Empty Visitor ########################################
#################################################################################
class NativeTypeVisitor:
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        raise NotImplementedError('Missing getKeyValDecl')
    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        raise NotImplementedError('Missing getKeyValInit')
    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        raise NotImplementedError('Missing getArrayLengthInit')
    def getMarkerInit(self, varname, size, forceNull):
        raise NotImplementedError('Missing getMarkerInit')
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        raise NotImplementedError('Missing getKeyValSetup')
    def getKeyValSet(self, basename, indexStr):
        raise NotImplementedError('Missing getKeyValSet')
    def getSig(self, basename, isKey):
        raise NotImplementedError()
    def getWriteWithOffsetSeg(self, basename, isKey):
        raise NotImplementedError()
    def getWriteMethodBody(self, basename, isKey):
        raise NotImplementedError()
    def getWriteWithOffsetMethodBody(self, basename, isKey):
        raise NotImplementedError()
    def getIterArg(self, ):
        raise NotImplementedError()
    def getKernelCall(self, basename, isKey):
        raise NotImplementedError()
    def getKernelCallIter(self):
        raise NotImplementedError(self)
    def getOriginalInitMethod():
        raise NotImplementedError(self)
    def getSetupParameter(self, basename, isInput, isKey):
        raise NotImplementedError()
    def getSetLengths(self):
        raise NotImplementedError()
    def getAddValueMethodMapper(self):
        raise NotImplementedError()
    def getAddValueMethodReducer(self):
        raise NotImplementedError()
    def getAddKeyMethod(self, index):
        raise NotImplementedError()
    def getLimitSetter(self):
        raise NotImplementedError()
    def getCloneIncompleteMapperKey(self):
        raise NotImplementedError()
    def getCloneIncompleteMapperValue(self):
        raise NotImplementedError()
    def getCloneIncompleteReducerKey(self):
        raise NotImplementedError()
    def getCloneIncompleteReducerValue(self):
        raise NotImplementedError()
    def getFillParameter(self, basename, isInput, isMapper):
        raise NotImplementedError()
    def getMapArguments(self, varName):
        raise NotImplementedError()
    def getBufferedDecl(self):
        raise NotImplementedError()
    def getBufferInputValue(self):
        raise NotImplementedError()
    def getUseBufferedValues(self):
        raise NotImplementedError()
    def getBufferedInit(self):
        raise NotImplementedError()
    def getResetHelper(self):
        raise NotImplementedError()
    def getAddValHelper(self):
        raise NotImplementedError()
    def getJavaProcessReducerCall(self):
        raise NotImplementedError()
    def getSpace(self, isMapper, isInput, isKey):
        raise NotImplementedError()
    def getOutputLength(self, core, count):
        raise NotImplementedError()
    def getInputLength(self, core, isMapper):
        raise NotImplementedError()

#################################################################################
########################## Visitor for Primitive type ###########################
#################################################################################
class PrimitiveVisitor(NativeTypeVisitor):
    def __init__(self, typ):
        self.typ = typ

    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        return [ 'public '+self.typ+'[] '+basename+'s;' ]
    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        if forceNull:
            initializer = 'null'
        else:
            initializer = 'new '+self.typ+'['+size+']'
        return [ basename+'s = '+initializer+';' ]
    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        return [ 'this.arrayLengths.put("'+basename+'s", '+size+');' ]
    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        if forceNull:
            return [ 'this.'+basename+'s = null;' ]
        else:
            return [ 'this.'+basename+'s = set'+basename.capitalize()+'s;' ]
    def getKeyValSet(self, basename, indexStr):
        return [ 'save'+basename+'.set(this.output'+basename+'s['+indexStr+']);' ]
    def getSig(self, basename, isKey):
        return [ self.typ+' '+basename ]
    def getWriteMethodBody(self, basename, isKey):
        return [ basename+'Obj.set('+basename+');' ]
    def getIterArg(self):
        return [ 'HadoopCL'+self.typ.capitalize()+'ValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        return [ 'input'+basename+'s[3]' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCL'+self.typ.capitalize()+
        #          'ValueIterator(inputVals, stopOffset-startOffset)' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizable'+self.typ.capitalize()+'Array();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        return [ self.typ+'[] set'+basename.capitalize()+'s' ]
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputVals");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputVals[this.nPairs] = actual.get();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputVals[this.nVals++] = actual.get();' ]
    def getAddKeyMethod(self, index):
        return [ 'this.inputKeys['+index+'] = actual.get();' ]
    def getLimitSetter(self):
        return [ 'int limit = this.outputKeys.length < this.memIncr[0] ? this.outputKeys.length : this.memIncr[0];' ]
    def getCloneIncompleteMapperKey(self):
        return [ 'newBuffer.inputKeys[newBuffer.nPairs] = this.inputKeys[i];' ]
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputVals[newBuffer.nPairs] = this.inputVals[i];' ]
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeys[newBuffer.nKeys] = this.inputKeys[i];' ]
    def getCloneIncompleteReducerValue(self):
        return [ 'System.arraycopy(this.inputVals, baseValOffset, newBuffer.inputVals, newBuffer.nVals, topValOffset - baseValOffset);' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'s' ]
    def getMapArguments(self, varName):
        return [ varName+'.get()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizable'+self.typ.capitalize()+'Array bufferedVals = null;' ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizable'+self.typ.capitalize()+'Array)this.tempBuffer1).add(actual.get());' ]
    def getUseBufferedValues(self):
        return [ 'System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputVals, this.nVals, this.tempBuffer1.size());' ]
    def getBufferedInit(self):
        return [ 'this.bufferedVals = new HadoopCLResizable'+self.typ.capitalize()+'Array();' ]
    def getResetHelper(self):
        return [ 'this.bufferedVals.reset();' ]
    def getAddValHelper(self):
        return [ 'this.bufferedVals.add(v.get());' ]
    def getJavaProcessReducerCall(self):
        return [ 'new HadoopCL'+self.typ.capitalize()+
                'ValueIterator(('+self.typ+
                '[])this.bufferedVals.getArray(), this.bufferedVals.size()));' ]
    def getSpace(self, isMapper, isInput, isKey):
        directionStr = 'input' if isInput else 'output'
        valStr = 'Keys' if isKey else 'Vals'
        space = '('+directionStr+valStr+'.length * '+str(typeToSize(self.typ))+')'
        if isKey:
            space = space + ' +'
        else:
            space = space + ';'
        return [ space ]
    def getOutputLength(self, core, count):
        return [ count+'+"/"+this.output'+core+'s.length+" '+core.lower()+'"' ]
    def getInputLength(self, core, isMapper):
        if isMapper:
            return [ 'this.nPairs+"/"+this.input'+core+'s.length+" '+core.lower()+'s"' ]
        else:
            if core == 'Key':
                return [ 'this.nKeys+"/"+this.input'+core+'s.length+" '+core.lower()+'s"' ]
            else:
                return [ 'this.nVals+"/"+this.input'+core+'s.length+" '+core.lower()+'s"' ]


#################################################################################
########################## Visitor for Pair type ################################
#################################################################################
class PairVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        return [ 'public double[] '+basename+'s1;',
                 'public double[] '+basename+'s2;' ]
    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        if forceNull:
            initializer = 'null'
        else:
            initializer = 'new double['+size+']'
        return [ basename+'s1 = '+initializer+';',
                 basename+'s2 = '+initializer+';' ]
    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        return [ 'this.arrayLengths.put("'+basename+'s1", '+size+');',
                 'this.arrayLengths.put("'+basename+'s2", '+size+');' ]
    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        if forceNull:
            return [ 'this.'+basename+'s1 = null;',
                     'this.'+basename+'s2 = null;' ]
        else:
            return [ 'this.'+basename+'s1 = set'+basename.capitalize()+'s1;',
                     'this.'+basename+'s2 = set'+basename.capitalize()+'s2;' ]
    def getKeyValSet(self, basename, indexStr):
        return [ 'save'+basename+'.set(this.output'+basename+'s1['+indexStr+'], this.output'+basename+'s2['+indexStr+']);' ]
    def getSig(self, basename, isKey):
        return [ 'double '+basename+'1, double '+basename+'2' ]
    def getWriteMethodBody(self, basename, isKey):
        return [ basename+'Obj.set('+basename+'1, '+basename+'2);' ]
    def getIterArg(self):
        return [ 'HadoopCLPairValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        return [ 'input'+basename+'s1[3], input'+basename+'s2[3]' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCLPairValueIterator(inputVals1, inputVals2, stopOffset-startOffset)' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizableDoubleArray();',
                 'this.tempBuffer2 = new HadoopCLResizableDoubleArray();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        return [ 'double[] set'+basename.capitalize()+'s1, ',
                 'double[] set'+basename.capitalize()+'s2' ]
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputVals1");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputVals1[this.nPairs] = actual.getVal1();',
                 'this.inputVals2[this.nPairs] = actual.getVal2();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputVals1[this.nVals] = actual.getVal1();',
                 'this.inputVals2[this.nVals++] = actual.getVal2();' ]
    def getAddKeyMethod(self, index):
        return [ 'this.inputKeys1['+index+'] = actual.getVal1();',
                 'this.inputKeys2['+index+'] = actual.getVal2();' ]
    def getLimitSetter(self):
        return [ 'int limit = this.outputKeys1.length < this.memIncr[0] ? this.outputKeys1.length : this.memIncr[0];' ]
    def getCloneIncompleteMapperKey(self):
        return [ 'newBuffer.inputKeys1[newBuffer.nPairs] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nPairs] = this.inputKeys2[i];' ]
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputVals1[newBuffer.nPairs] = this.inputVals1[i];',
                 'newBuffer.inputVals2[newBuffer.nPairs] = this.inputVals2[i];' ]
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];' ]
    def getCloneIncompleteReducerValue(self):
        return [ 'System.arraycopy(this.inputVals1, baseValOffset, newBuffer.inputVals1, newBuffer.nVals, topValOffset - baseValOffset);',
                 'System.arraycopy(this.inputVals2, baseValOffset, newBuffer.inputVals2, newBuffer.nVals, topValOffset - baseValOffset);' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'s1, '+basename+'s2' ]
    def getMapArguments(self, varName):
        return [ varName+'.getVal1(), '+varName+'.getVal2()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizableDoubleArray bufferedVal1 = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal2 = null;' ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizableDoubleArray)this.tempBuffer1).add(actual.getVal1());',
                 '((HadoopCLResizableDoubleArray)this.tempBuffer2).add(actual.getVal2());' ]
    def getUseBufferedValues(self):
        return [ 'System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputVals1, this.nVals, this.tempBuffer1.size());',
                 'System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVals2, this.nVals, this.tempBuffer2.size());' ]
    def getBufferedInit(self):
        return [ 'this.bufferedVal1 = new HadoopCLResizableDoubleArray();',
                 'this.bufferedVal2 = new HadoopCLResizableDoubleArray();' ]
    def getResetHelper(self):
        return [ 'this.bufferedVal1.reset();',
                 'this.bufferedVal2.reset();' ]
    def getAddValHelper(self):
        return [ 'this.bufferedVal1.add(v.getVal1());',
                 'this.bufferedVal2.add(v.getVal2());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLPairValueIterator(
                       (double[])this.bufferedVal1.getArray(),
                       (double[])this.bufferedVal2.getArray(),
                       this.bufferedVal1.size()));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        directionStr = 'input' if isInput else 'output'
        valStr = 'Keys' if isKey else 'Vals'
        base = directionStr + valStr
        if isKey:
            return [ '('+base+'1.length * 8) +',
                     '('+base+'2.length * 8) +' ]
        else:
            return [ '('+base+'1.length * 8) +',
                     '('+base+'2.length * 8);' ]
    def getOutputLength(self, core, count):
        return [ count+'+"/"+this.output'+core+'s1.length+" pair '+core.lower()+'"' ]
    def getInputLength(self, core, isMapper):
        if isMapper:
            return [ 'this.nPairs+"/"+this.input'+core+'s1.length+" '+core.lower()+'s"' ]
        else:
            if core == 'Key':
                return [ 'this.nKeys+"/"+this.input'+core+'s1.length+" '+core.lower()+'s"' ]
            else:
                return [ 'this.nVals+"/"+this.input'+core+'s1.length+" '+core.lower()+'s"' ]

#################################################################################
########################## Visitor for Ipair type ###############################
#################################################################################
class IpairVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        return [ 'public int[] '+basename+'Ids;',
                 'public double[] '+basename+'s1;',
                 'public double[] '+basename+'s2;' ]
    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        if forceNull:
            initializer1 = initializer2 = 'null';
        else:
            initializer1 = 'new int['+size+']';
            initializer2 = 'new double['+size+']';
        return [ basename+'Ids = '+initializer1+';',
                 basename+'s1 = '+initializer2+';',
                 basename+'s2 = '+initializer2+';' ]
    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        return [ 'this.arrayLengths.put("'+basename+'Ids", '+size+');',
                 'this.arrayLengths.put("'+basename+'s1", '+size+');',
                 'this.arrayLengths.put("'+basename+'s2", '+size+');' ]
    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        if forceNull:
            return [ 'this.'+basename+'Ids = null;',
                     'this.'+basename+'s1 = null;',
                     'this.'+basename+'s2 = null;' ]
        else:
            return [ 'this.'+basename+'Ids = set'+basename.capitalize()+'Ids;',
                     'this.'+basename+'s1 = set'+basename.capitalize()+'s1;',
                     'this.'+basename+'s2 = set'+basename.capitalize()+'s2;' ]
    def getKeyValSet(self, basename, indexStr):
        return [ 'save'+basename+'.set(this.output'+basename+'Ids['+indexStr+'], this.output'+basename+'s1['+indexStr+'], this.output'+basename+'s2['+indexStr+']);' ]
    def getSig(self, basename, isKey):
        return [ 'int '+basename+'Id, double '+basename+'1, double '+basename+'2' ]
    def getWriteMethodBody(self, basename, isKey):
        return [ basename+'Obj.set('+basename+'Id, '+basename+'1, '+basename+'2);' ]
    def getIterArg(self):
        return [ 'HadoopCLUPairValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        return [ 'input'+basename+'Ids[3], input'+basename+'s1[3], input'+basename+'s2[3]' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCLUPairValueIterator(inputValIds, inputVals1, inputVals2, stopOffset-startOffset)' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer2 = new HadoopCLResizableDoubleArray();',
                 'this.tempBuffer3 = new HadoopCLResizableDoubleArray();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        return [ 'int[] set'+basename.capitalize()+'Ids, ',
                 'double[] set'+basename.capitalize()+'s1, ',
                 'double[] set'+basename.capitalize()+'s2' ]
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputValIds");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputValIds[this.nPairs] = actual.getIVal();',
                 'this.inputVals1[this.nPairs] = actual.getVal1();',
                 'this.inputVals2[this.nPairs] = actual.getVal2();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValIds[this.nVals] = actual.getIVal();',
                 'this.inputVals1[this.nVals] = actual.getVal1();',
                 'this.inputVals2[this.nVals++] = actual.getVal2();' ]
    def getAddKeyMethod(self, index):
        return [ 'this.inputKeyIds['+index+'] = actual.getIVal();',
                 'this.inputKeys1['+index+'] = actual.getVal1();',
                 'this.inputKeys2['+index+'] = actual.getVal2();' ]
    def getLimitSetter(self):
        return [ 'int limit = this.outputKeyIds.length < this.memIncr[0] ? this.outputKeyIds.length : this.memIncr[0];' ]
    def getCloneIncompleteMapperKey(self):
        return [ 'newBuffer.inputKeyIds[newBuffer.nPairs] = this.inputKeyIds[i];',
                 'newBuffer.inputKeys1[newBuffer.nPairs] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nPairs] = this.inputKeys2[i];' ]
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputValIds[newBuffer.nPairs] = this.inputValIds[i];',
                 'newBuffer.inputVals1[newBuffer.nPairs] = this.inputVals1[i];',
                 'newBuffer.inputVals2[newBuffer.nPairs] = this.inputVals2[i];' ]
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeyIds[newBuffer.nKeys] = this.inputKeyIds[i];',
                 'newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];' ]
    def getCloneIncompleteReducerValue(self):
        return [ 'System.arraycopy(this.inputValIds, baseValOffset, newBuffer.inputValIds, newBuffer.nVals, topValOffset - baseValOffset);',
                 'System.arraycopy(this.inputVals1, baseValOffset, newBuffer.inputVals1, newBuffer.nVals, topValOffset - baseValOffset);',
                 'System.arraycopy(this.inputVals2, baseValOffset, newBuffer.inputVals2, newBuffer.nVals, topValOffset - baseValOffset);' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'Ids, '+basename+'s1, '+basename+'s2' ]
    def getMapArguments(self, varName):
        return [ varName+'.getIVal(), '+varName+'.getVal1(), '+varName+'.getVal2()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizableIntArray bufferedValId = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal1 = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal2 = null;' ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizableIntArray)this.tempBuffer1).add(actual.getIVal());',
                 '((HadoopCLResizableDoubleArray)this.tempBuffer2).add(actual.getVal1());',
                 '((HadoopCLResizableDoubleArray)this.tempBuffer3).add(actual.getVal2());' ]
    def getUseBufferedValues(self):
        return [ 'System.arraycopy(this.tempBuffer1.getArray(), 0, this.inputValIds, this.nVals, this.tempBuffer1.size());',
                 'System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVals1, this.nVals, this.tempBuffer2.size());',
                 'System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputVals2, this.nVals, this.tempBuffer3.size());' ]
    def getBufferedInit(self):
        return [ 'this.bufferedValId = new HadoopCLResizableIntArray();',
                 'this.bufferedVal1 = new HadoopCLResizableDoubleArray();',
                 'this.bufferedVal2 = new HadoopCLResizableDoubleArray();' ]
    def getResetHelper(self):
        return [ 'this.bufferedValId.reset();',
                 'this.bufferedVal1.reset();',
                 'this.bufferedVal2.reset();' ]
    def getAddValHelper(self):
        return [ 'this.bufferedValId.add(v.getIVal());',
                 'this.bufferedVal1.add(v.getVal1());',
                 'this.bufferedVal2.add(v.getVal2());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLUPairValueIterator(
                       (int[])this.bufferedValId.getArray(),
                       (double[])this.bufferedVal1.getArray(),
                       (double[])this.bufferedVal2.getArray(),
                       this.bufferedValId.size()));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        directionStr = 'input' if isInput else 'output'
        valStr = 'Key' if isKey else 'Val'
        base = directionStr + valStr
        if isKey:
            return [ '('+base+'s1.length * 8) +',
                     '('+base+'s2.length * 8) +',
                     '('+base+'Ids.length * 4) +' ]
        else:
            return [ '('+base+'s1.length * 8) +',
                     '('+base+'s2.length * 8) +',
                     '('+base+'Ids.length * 4);' ]
    def getOutputLength(self, core, count):
        return [ count+'+"/"+this.output'+core+'s1.length+" ipair '+core.lower()+'"' ]
    def getInputLength(self, core, isMapper):
        if isMapper:
            return [ 'this.nPairs+"/"+this.input'+core+'s1.length+" '+core.lower()+'s"' ]
        else:
            if core == 'Key':
                return [ 'this.nKeys+"/"+this.input'+core+'s1.length+" '+core.lower()+'s"' ]
            else:
                return [ 'this.nVals+"/"+this.input'+core+'s1.length+" '+core.lower()+'s"' ]

#################################################################################
########################## Visitor for Svec type ################################
#################################################################################
class SvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        buf = [ ]
        if isInput:
            buf.append('public int[] '+basename+'LookAsideBuffer;')
            buf.append('public int[] '+basename+'Indices;')
            buf.append('public double[] '+basename+'Vals;')
        else:
            buf.append('public int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public int[] '+basename+'DoubleLookAsideBuffer;')
            buf.append('public int[] '+basename+'Indices;')
            buf.append('public double[] '+basename+'Vals;')

        if not isInput:
            buf.append('private int[] bufferOutputIndices = null;')
            buf.append('private double[] bufferOutputVals = null;')
            buf.append('public int[] outputValLengthBuffer;')
        return buf

    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        buf = [ ]
        if forceNull:
            if isInput:
                buf.append(basename+'LookAsideBuffer = null;')
            else:
                buf.append(basename+'IntLookAsideBuffer = null;')
                buf.append(basename+'DoubleLookAsideBuffer = null;')
            buf.append(basename+'Indices = null;')
            buf.append(basename+'Vals = null;')
        else:
            if isInput:
                buf.append(basename+'LookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'DoubleLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new double[this.clContext.getPreallocDoubleLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
            buf.append('this.nVectorsToBuffer = clContext.getNVectorsToBuffer();')
        # if isMapper and not isInput and not isKey:
        if not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize() * outputsPerInput];')
            buf.append('memAuxIntIncr = new int[1];')
            buf.append('memAuxDoubleIncr = new int[1];')
        return buf
    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        buf = [ ]
        buf.append('this.arrayLengths.put("'+basename+'IntLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'DoubleLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'Indices", this.clContext.getPreallocIntLength());')
        buf.append('this.arrayLengths.put("'+basename+'Vals", this.clContext.getPreallocDoubleLength());')
        if not isKey:
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize() * this.getOutputPairsPerInput());')
            buf.append('this.arrayLengths.put("memAuxIntIncr", 1);')
            buf.append('this.arrayLengths.put("memAuxDoubleIncr", 1);')

        return buf
    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        buf = [ ]
        if isInput:
            if forceNull:
                buf.append('this.'+basename+'LookAsideBuffer = null;')
            else:
                buf.append('this.'+basename+'LookAsideBuffer = set'+basename.capitalize()+'LookAsideBuffer;')
        else:
            if forceNull:
                buf.append('this.'+basename+'IntLookAsideBuffer = null;')
                buf.append('this.'+basename+'DoubleLookAsideBuffer = null;')
            else:
                buf.append('this.'+basename+'IntLookAsideBuffer = set'+basename.capitalize()+'IntLookAsideBuffer;')
                buf.append('this.'+basename+'DoubleLookAsideBuffer = set'+basename.capitalize()+'DoubleLookAsideBuffer;')
        if forceNull:
            buf.append('this.'+basename+'Indices = null;')
            buf.append('this.'+basename+'Vals = null;')
        else:
            buf.append('this.'+basename+'Indices = set'+basename.capitalize()+'Indices;')
            buf.append('this.'+basename+'Vals = set'+basename.capitalize()+'Vals;')

        if not isKey and not isInput:
            if forceNull:
                buf.append('this.outputValLengthBuffer = null;')
            else:
                buf.append('this.outputValLengthBuffer = setOutputValLengthBuffer;')
        return buf

    def getSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type svec')
        return [ 'int[] '+basename+'Indices, double[] '+basename+'Vals, int len' ]
    def getWriteWithOffsetSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type svec')
        return [ 'int[] '+basename+'Indices, int indicesOffset, double[] '+basename+'Vals, int valsOffset, int len' ]
    def getWriteMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type svec')
        return [ basename+'Obj.set('+basename+'Indices, '+basename+'Vals, len);' ]
    def getWriteWithOffsetMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type svec')
        return [ basename+'Obj.set('+basename+'Indices, indicesOffset, '+basename+'Vals, valsOffset, len);' ]
    def getIterArg(self):
        return [ 'HadoopCLSvecValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type svec')
        return [ 'input'+basename+'Indices, input'+basename+'Vals, input'+basename+'LookAsideBuffer[3] + this.nPairs + this.individualInputValsCount' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCLSvecValueIterator(null, null)' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer2 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer3 = new HadoopCLResizableDoubleArray();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        buf = [ ]
        if isInput:
            buf.append('int[] set'+basename.capitalize()+'LookAsideBuffer, ')
        else:
            buf.append('int[] set'+basename.capitalize()+'IntLookAsideBuffer, ')
            buf.append('int[] set'+basename.capitalize()+'DoubleLookAsideBuffer, ')
        buf.append('int[] set'+basename.capitalize()+'Indices, ')
        buf.append('double[] set'+basename.capitalize()+'Vals')
        if not isInput and not isKey:
            buf.append(', int[] setOutputValLengthBuffer')
        return buf
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputValIntLookAsideBuffer");',
                 'this.outputAuxIntLength = this.getArrayLength("outputValIndices");',
                 'this.outputAuxDoubleLength = this.getArrayLength("outputValVals");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;',
                 'if (this.enableStriding) {',
                 '    IndValWrapper wrapper = new IndValWrapper(actual.indices(), actual.vals(), actual.size());',
                 '    if (this.sortedVals.containsKey(actual.size())) {',
                 '        this.sortedVals.get(actual.size()).add(wrapper);',
                 '    } else {',
                 '        LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();',
                 '        newList.add(wrapper);',
                 '        this.sortedVals.put(actual.size(), newList);',
                 '    }',
                 '} else {',
                 '    System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputValIndices.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.indices()[i]);',
                 # '        this.inputValVals.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
#    def getAddKeyMethod(self, index):
#    def getLimitSetter():
#    def getCloneIncompleteMapperKey():
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputValLookAsideBuffer[newBuffer.nPairs] = newBuffer.individualInputValsCount;',
                 'int length;',
                 'if (this.enableStriding) {',
                 '    length = (i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1]) - this.inputValLookAsideBuffer[i];',
                 '    for (int j = 0; j < length; j++) {',
                 '        newBuffer.inputValIndices.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputValIndices.get(i + (j * this.nPairs)));',
                 '        newBuffer.inputValVals.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputValVals.get(i + (j * this.nPairs)));',
                 '    }',
                 '} else {',
                 '    int baseOffset = this.inputValLookAsideBuffer[i];',
                 '    int topOffset = i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1];',
                 '    length = topOffset - baseOffset;',
                 '    System.arraycopy((int[])(this.inputValIndices.getArray()),',
                 '        baseOffset, (int[])(newBuffer.inputValIndices.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '    System.arraycopy((double[])(this.inputValVals.getArray()),',
                 '        baseOffset, (double[])(newBuffer.inputValVals.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '}',
                 'newBuffer.individualInputValsCount += length;' ]
#    def getCloneIncompleteReducerKey():
    def getCloneIncompleteReducerValue(self):
        return [ 'for(int j = baseValOffset; j < topValOffset; j++) {',
                 '    int offsetInNewBuffer = newBuffer.nVals + j-baseValOffset;',
                 '    newBuffer.inputValLookAsideBuffer[offsetInNewBuffer] = newBuffer.individualInputValsCount;',
                 '    int baseOffset = this.inputValLookAsideBuffer[j];',
                 '    int topOffset = j == this.nVals-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[j+1];',
                 '    System.arraycopy(this.inputValIndices, baseOffset, newBuffer.inputValIndices, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    System.arraycopy(this.inputValVals, baseOffset, newBuffer.inputValVals, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    newBuffer.individualInputValsCount += (topOffset - baseOffset);',
                 '}' ]
    def getFillParameter(self, basename, isInput, isMapper):
        buf = [ ]
        if isInput:
            buf.append(basename+'LookAsideBuffer, '+basename+'Indices, '+basename+'Vals')
        else:
            buf.append(basename+'IntLookAsideBuffer, '+basename+'DoubleLookAsideBuffer, '+basename+'Indices, '+basename+'Vals')
        return buf
    def getMapArguments(self, varName):
        return [ varName+'.indices(), '+varName+'.vals(), '+varName+'.size()' ]
    def getBufferedDecl(self):
        return [ ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizableIntArray)this.tempBuffer1).add(this.tempBuffer2.size());',
                 'for(int i = 0; i < actual.size(); i++) {',
                 '    ((HadoopCLResizableIntArray)this.tempBuffer2).add(actual.indices()[i]);',
                 '    ((HadoopCLResizableDoubleArray)this.tempBuffer3).add(actual.vals()[i]);',
                 '}' ]
    def getUseBufferedValues(self):
        return [ 'for(int i = 0; i < this.tempBuffer1.size(); i++) {',
                 '    this.inputValLookAsideBuffer[this.nVals + i] = this.individualInputValsCount + ((int[])this.tempBuffer1.getArray())[i];',
                 '}',
                 'System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputValIndices, this.individualInputValsCount, this.tempBuffer2.size());',
                 'System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputValVals, this.individualInputValsCount, this.tempBuffer3.size());',
                 'this.individualInputValsCount += this.tempBuffer2.size();' ]
    def getBufferedInit(self):
        return [ ]
    def getResetHelper(self):
        return [ 'List<int[]> accIndices = new ArrayList<int[]>();',
                 'List<double[]> accVals = new ArrayList<double[]>();' ]
    def getAddValHelper(self):
        return [ 'accIndices.add(v.indices());',
                 'accVals.add(v.vals());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLSvecValueIterator(
                       accIndices, accVals));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        # Can't be a key, so isKey always == False
        if isInput:
            if isMapper:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
            else:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
        else:
            return [ '(outputValIntLookAsideBuffer.length * 4) +',
                     '(outputValDoubleLookAsideBuffer.length * 4) +',
                     '(outputValIndices.length * 4) +',
                     '(outputValVals.length * 8) +',
                     '(outputValLengthBuffer.length * 4) +',
                     '(bufferOutputIndices == null ? 0 : bufferOutputIndices.length * 4) +',
                     '(bufferOutputVals == null ? 0 : bufferOutputVals.length * 8) +',
                     '(memAuxIntIncr.length * 4) +',
                     '(memAuxDoubleIncr.length * 4);' ]
    def getOutputLength(self, core, count):
        return [ '(this.output'+core+'IntLookAsideBuffer['+count+'-1]+',
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Indices.length+" int memory, and "+',
                 '(this.output'+core+'DoubleLookAsideBuffer['+count+'-1]+'
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Vals.length+" double memory"' ]
    def getInputLength(self, core, isMapper):
        return [ 'this.individualInputValsCount+"/"+this.input'+core+'Indices.length+" '+core.lower()+'s elements"' ]

#################################################################################
########################## Visitor for Ivec type ################################
#################################################################################
class IvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        buf = [ ]
        if isInput:
            buf.append('public int[] '+basename+'LookAsideBuffer;')
            buf.append('public int[] '+basename+';')
        else:
            buf.append('public int[] '+basename+'LookAsideBuffer;')
            buf.append('public int[] '+basename+';')

        if not isInput:
            buf.append('private int[] bufferOutput = null;')
            buf.append('public int[] outputLengthBuffer;')
        return buf

    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        buf = [ ]
        if forceNull:
            buf.append(basename+'LookAsideBuffer = null;')
            buf.append(basename+' = null;')
        else:
            if isInput:
                buf.append(basename+'LookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+' = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
            else:
                buf.append(basename+'LookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+' = new int[this.clContext.getPreallocIntLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
            buf.append('this.nVectorsToBuffer = clContext.getNVectorsToBuffer();')
            buf.append('System.err.println("Setting nVectorsToBuffer to "+this.nVectorsToBuffer);')
        if isMapper and not isInput and not isKey:
            buf.append('outputLengthBuffer = new int['+size+'];')
            buf.append('memAuxIncr = new int[1];')
        return buf
    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        buf = [ ]
        buf.append('this.arrayLengths.put("'+basename+'LookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'", this.clContext.getPreallocIntLength());')
        if isMapper and not isKey:
            buf.append('this.arrayLengths.put("outputLengthBuffer", '+size+');')
            buf.append('this.arrayLengths.put("memAuxIncr", 1);')
        return buf

    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        buf = [ ]
        if forceNull:
            buf.append('this.'+basename+'LookAsideBuffer = null;')
            buf.append('this.'+basename+' = null;')
        else:
            buf.append('this.'+basename+'LookAsideBuffer = set'+basename.capitalize()+'LookAsideBuffer;')
            buf.append('this.'+basename+' = set'+basename.capitalize()+';')

        if not isKey and not isInput:
            if forceNull:
                buf.append('this.outputLengthBuffer = null;')
            else:
                buf.append('this.outputLengthBuffer = setOutputLengthBuffer;')
        return buf

    def getSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type svec')
        return [ 'int[] '+basename+', int len' ]
    def getWriteWithOffsetSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type svec')
        return [ 'int[] '+basename+', int offset, int len' ]
    def getWriteMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type svec')
        return [ basename+'Obj.set('+basename+', len);' ]
    def getWriteWithOffsetMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type svec')
        return [ basename+'Obj.set('+basename+', offset, len);' ]
    def getIterArg(self):
        return [ 'HadoopCLIvecValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type svec')
        return [ 'input'+basename+', input'+basename+'LookAsideBuffer[3] + this.nPairs + this.individualInputValsCount' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCLIvecValueIterator(null)' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer2 = new HadoopCLResizableIntArray();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        buf = [ ]
        buf.append('int[] set'+basename.capitalize()+'LookAsideBuffer, ')
        buf.append('int[] set'+basename.capitalize())
        if not isInput and not isKey:
            buf.append(', int[] setOutputLengthBuffer')
        return buf
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputLookAsideBuffer");',
                 'this.outputAuxIntLength = this.getArrayLength("output");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;',
                 'if (this.enableStriding) {',
                 '    IndValWrapper wrapper = new IndValWrapper(actual.vals(), actual.size());',
                 '    if (this.sortedVals.containsKey(actual.size())) {',
                 '        this.sortedVals.get(actual.size()).add(wrapper);',
                 '    } else {',
                 '        LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();',
                 '        newList.add(wrapper);',
                 '        this.sortedVals.put(actual.size(), newList);',
                 '    }',
                 # '    this.inputVal.ensureCapacity(this.nPairs +',
                 # '        (actual.size()  * nVectorsToBuffer));',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputVal.unsafeSet(this.nPairs + (i * nVectorsToBuffer),',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '} else {',
                 '    System.arraycopy(actual.vals(), 0, this.inputVal, this.individualInputValsCount, actual.size());',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputVal.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.getArray(), 0, this.inputVal, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
#    def getAddKeyMethod(self, index):
#    def getLimitSetter():
#    def getCloneIncompleteMapperKey():
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputValLookAsideBuffer[newBuffer.nPairs] = newBuffer.individualInputValsCount;',
                 'int length;',
                 'if (this.enableStriding) {',
                 '    length = (i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1]) - this.inputValLookAsideBuffer[i];',
                 '    for (int j = 0; j < length; j++) {',
                 '        newBuffer.inputVal.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputVal.get(i + (j * this.nPairs)));',
                 '    }',
                 '} else {',
                 '    int baseOffset = this.inputValLookAsideBuffer[i];',
                 '    int topOffset = i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1];',
                 '    length = topOffset - baseOffset;',
                 '    System.arraycopy((int[])(this.inputVal.getArray()),',
                 '        baseOffset, (int[])(newBuffer.inputVal.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '}',
                 'newBuffer.individualInputValsCount += length;' ]
#    def getCloneIncompleteReducerKey():
    def getCloneIncompleteReducerValue(self):
        return [ 'for(int j = baseValOffset; j < topValOffset; j++) {',
                 '    int offsetInNewBuffer = newBuffer.nVals + j-baseValOffset;',
                 '    newBuffer.inputValLookAsideBuffer[offsetInNewBuffer] = newBuffer.individualInputValsCount;',
                 '    int baseOffset = this.inputValLookAsideBuffer[j];',
                 '    int topOffset = j == this.nVals-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[j+1];',
                 '    System.arraycopy(this.inputVal, baseOffset, newBuffer.inputVal, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    newBuffer.individualInputValsCount += (topOffset - baseOffset);',
                 '}' ]
    def getFillParameter(self, basename, isInput, isMapper):
        buf = [ ]
        if isInput:
            buf.append(basename+'LookAsideBuffer, '+basename)
        else:
            buf.append(basename+'IntLookAsideBuffer, '+basename+'DoubleLookAsideBuffer, '+basename)
        return buf
    def getMapArguments(self, varName):
        return [ varName+'.vals(), '+varName+'.size()' ]
    def getBufferedDecl(self):
        return [ ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizableIntArray)this.tempBuffer1).add(this.tempBuffer2.size());',
                 'for(int i = 0; i < actual.size(); i++) {',
                 '    ((HadoopCLResizableIntArray)this.tempBuffer2).add(actual.getArray()[i]);',
                 '}' ]
    def getUseBufferedValues(self):
        return [ 'for(int i = 0; i < this.tempBuffer1.size(); i++) {',
                 '    this.inputValLookAsideBuffer[this.nVals + i] = this.individualInputValsCount + ((int[])this.tempBuffer1.getArray())[i];',
                 '}',
                 'System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputVal, this.individualInputValsCount, this.tempBuffer2.size());',
                 'this.individualInputValsCount += this.tempBuffer2.size();' ]
    def getBufferedInit(self):
        return [ ]
    def getResetHelper(self):
        return [ 'List<int[]> acc = new ArrayList<int[]>();' ]
    def getAddValHelper(self):
        return [ 'acc.add(v.getArray());' ]
    def getJavaProcessReducerCall(self):
        return [ 'new HadoopCLIvecValueIterator(acc));' ]
    def getSpace(self, isMapper, isInput, isKey):
        if isInput:
            return [ '(inputValLookAsideBuffer.length * 4) +',
                     '(inputVal.length * 4);' ]
        else:
            return [ '(outputValIntLookAsideBuffer.length * 4) +',
                     '(outputVal.length * 4) +',
                     '(outputValLengthBuffer.length * 4) +',
                     '(bufferOutputVals == null ? 0 : bufferOutputVals.length * 8) +',
                     '(memAuxIntIncr.length * 4) +',
                     '(memAuxDoubleIncr.length * 4);' ]
    def getOutputLength(self, core, count):
        return [ '(this.output'+core+'IntLookAsideBuffer['+count+'-1]+',
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Indices.length+" int memory "' ]
    def getInputLength(self, core, isMapper):
        return [ 'this.individualInputValsCount+"/"+this.input'+core+'.length+" '+core.lower()+'s elements"' ]

#################################################################################
########################## Visitor for Fsvec type ###############################
#################################################################################
class FsvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        buf = [ ]
        if isInput:
            buf.append('public int[] '+basename+'LookAsideBuffer;')
            buf.append('public int[] '+basename+'Indices;')
            buf.append('public float[] '+basename+'Vals;')
        else:
            buf.append('public int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public int[] '+basename+'FloatLookAsideBuffer;')
            buf.append('public int[] '+basename+'Indices;')
            buf.append('public float[] '+basename+'Vals;')

        if not isInput:
            buf.append('private int[] bufferOutputIndices = null;')
            buf.append('private float[] bufferOutputVals = null;')
            buf.append('public int[] outputValLengthBuffer;')
        return buf

    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        buf = [ ]
        if forceNull:
            if isInput:
                buf.append(basename+'LookAsideBuffer = null;')
            else:
                buf.append(basename+'IntLookAsideBuffer = null;')
                buf.append(basename+'FloatLookAsideBuffer = null;')
            buf.append(basename+'Indices = null;')
            buf.append(basename+'Vals = null;')
        else:
            if isInput:
                buf.append(basename+'LookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                buf.append(basename+'Vals = new float[('+size+') * this.clContext.getInputValMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'FloatLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new float[this.clContext.getPreallocFloatLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
            buf.append('this.nVectorsToBuffer = clContext.getNVectorsToBuffer();')
            buf.append('System.err.println("Setting nVectorsToBuffer to "+this.nVectorsToBuffer);')
        if isMapper and not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize() * outputsPerInput];')
            buf.append('memAuxIntIncr = new int[1];')
            buf.append('memAuxFloatIncr = new int[1];')
        return buf

    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        buf = [ ]
        buf.append('this.arrayLengths.put("'+basename+'IntLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'FloatLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'Indices", this.clContext.getPreallocIntLength());')
        buf.append('this.arrayLengths.put("'+basename+'Vals", this.clContext.getPreallocFloatLength());')
        if isMapper and not isKey:
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize() * this.getOutputPairsPerInput());')
            buf.append('this.arrayLengths.put("memAuxIntIncr", 1);')
            buf.append('this.arrayLengths.put("memAuxFloatIncr", 1);')

        return buf

    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        buf = [ ]
        if isInput:
            if forceNull:
                buf.append('this.'+basename+'LookAsideBuffer = null;')
            else:
                buf.append('this.'+basename+'LookAsideBuffer = set'+basename.capitalize()+'LookAsideBuffer;')
        else:
            if forceNull:
                buf.append('this.'+basename+'IntLookAsideBuffer = null;')
                buf.append('this.'+basename+'FloatLookAsideBuffer = null;')
            else:
                buf.append('this.'+basename+'IntLookAsideBuffer = set'+basename.capitalize()+'IntLookAsideBuffer;')
                buf.append('this.'+basename+'FloatLookAsideBuffer = set'+basename.capitalize()+'FloatLookAsideBuffer;')

        if forceNull:
            buf.append('this.'+basename+'Indices = null;')
            buf.append('this.'+basename+'Vals = null;')
        else:
            buf.append('this.'+basename+'Indices = set'+basename.capitalize()+'Indices;')
            buf.append('this.'+basename+'Vals = set'+basename.capitalize()+'Vals;')

        if not isKey and not isInput:
            if forceNull:
                buf.append('this.outputValLengthBuffer = null;')
            else:
                buf.append('this.outputValLengthBuffer = setOutputValLengthBuffer;')
        return buf

    def getSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type fsvec')
        return [ 'int[] '+basename+'Indices, float[] '+basename+'Vals, int len' ]
    def getWriteWithOffsetSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type fsvec')
        return [ 'int[] '+basename+'Indices, int indicesOffset, float[] '+basename+'Vals, int valsOffset, int len' ]
    def getWriteMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type fsvec')
        return [ basename+'Obj.set('+basename+'Indices, '+basename+'Vals, len);' ]
    def getWriteWithOffsetMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type fsvec')
        return [ basename+'Obj.set('+basename+'Indices, indicesOffset, '+basename+'Vals, valsOffset, len);' ]
    def getIterArg(self):
        return [ 'HadoopCLFsvecValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type fsvec')
        return [ 'input'+basename+'Indices, input'+basename+'Vals, input'+basename+'LookAsideBuffer[3] + this.nPairs + this.individualInputValsCount' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCLFsvecValueIterator(null, null)' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer2 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer3 = new HadoopCLResizableFloatArray();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        buf = [ ]
        if isInput:
            buf.append('int[] set'+basename.capitalize()+'LookAsideBuffer, ')
        else:
            buf.append('int[] set'+basename.capitalize()+'IntLookAsideBuffer, ')
            buf.append('int[] set'+basename.capitalize()+'FloatLookAsideBuffer, ')
        buf.append('int[] set'+basename.capitalize()+'Indices, ')
        buf.append('float[] set'+basename.capitalize()+'Vals')
        if not isInput and not isKey:
            buf.append(', int[] setOutputValLengthBuffer')
        return buf
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputValIntLookAsideBuffer");',
                 'this.outputAuxIntLength = this.getArrayLength("outputValIndices");',
                 'this.outputAuxFloatLength = this.getArrayLength("outputValVals");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;',
                 'if (this.enableStriding) {',
                 '    IndValWrapper wrapper = new IndValWrapper(actual.indices(), actual.vals(), actual.size());',
                 '    if (this.sortedVals.containsKey(actual.size())) {',
                 '        this.sortedVals.get(actual.size()).add(wrapper);',
                 '    } else {',
                 '        LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();',
                 '        newList.add(wrapper);',
                 '        this.sortedVals.put(actual.size(), newList);',
                 '    }',
                 # '    this.inputValIndices.ensureCapacity( (this.nPairs +',
                 # '        ((actual.size() - 1) * nVectorsToBuffer)) + 1);',
                 # '    this.inputValVals.ensureCapacity( (this.nPairs +',
                 # '        ((actual.size() - 1) * nVectorsToBuffer)) + 1);',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputValIndices.unsafeSet(this.nPairs + (i * nVectorsToBuffer),',
                 # '            actual.indices()[i]);',
                 # '        this.inputValVals.unsafeSet(this.nPairs + (i * nVectorsToBuffer),',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '} else {',
                 '    System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputValIndices.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.indices()[i]);',
                 # '        this.inputValVals.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
#    def getAddKeyMethod(self, index):
#    def getLimitSetter():
#    def getCloneIncompleteMapperKey():
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputValLookAsideBuffer[newBuffer.nPairs] = newBuffer.individualInputValsCount;',
                 'int length;',
                 'if (this.enableStriding) {',
                 '    length = (i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1]) - this.inputValLookAsideBuffer[i];',
                 '    for (int j = 0; j < length; j++) {',
                 '        newBuffer.inputValIndices.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputValIndices.get(i + (j * this.nPairs)));',
                 '        newBuffer.inputValVals.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputValVals.get(i + (j * this.nPairs)));',
                 '    }',
                 '} else {',
                 '    int baseOffset = this.inputValLookAsideBuffer[i];',
                 '    int topOffset = i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1];',
                 '    length = topOffset - baseOffset;',
                 '    System.arraycopy((int[])(this.inputValIndices.getArray()),',
                 '        baseOffset, (int[])(newBuffer.inputValIndices.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '    System.arraycopy((float[])(this.inputValVals.getArray()),',
                 '        baseOffset, (float[])(newBuffer.inputValVals.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '}',
                 'newBuffer.individualInputValsCount += length;' ]
#    def getCloneIncompleteReducerKey():
    def getCloneIncompleteReducerValue(self):
        return [ 'for(int j = baseValOffset; j < topValOffset; j++) {',
                 '    int offsetInNewBuffer = newBuffer.nVals + j-baseValOffset;',
                 '    newBuffer.inputValLookAsideBuffer[offsetInNewBuffer] = newBuffer.individualInputValsCount;',
                 '    int baseOffset = this.inputValLookAsideBuffer[j];',
                 '    int topOffset = j == this.nVals-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[j+1];',
                 '    System.arraycopy(this.inputValIndices, baseOffset, newBuffer.inputValIndices, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    System.arraycopy(this.inputValVals, baseOffset, newBuffer.inputValVals, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    newBuffer.individualInputValsCount += (topOffset - baseOffset);',
                 '}' ]
    def getFillParameter(self, basename, isInput, isMapper):
        buf = [ ]
        if isInput:
            buf.append(basename+'LookAsideBuffer, '+basename+'Indices, '+basename+'Vals')
        else:
            buf.append(basename+'IntLookAsideBuffer, '+basename+'FloatLookAsideBuffer, '+basename+'Indices, '+basename+'Vals')
        return buf
    def getMapArguments(self, varName):
        return [ varName+'.indices(), '+varName+'.vals(), '+varName+'.size()' ]
    def getBufferedDecl(self):
        return [ ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizableIntArray)this.tempBuffer1).add(this.tempBuffer2.size());',
                 'for(int i = 0; i < actual.size(); i++) {',
                 '    ((HadoopCLResizableIntArray)this.tempBuffer2).add(actual.indices()[i]);',
                 '    ((HadoopCLResizableFloatArray)this.tempBuffer3).add(actual.vals()[i]);',
                 '}' ]
    def getUseBufferedValues(self):
        return [ 'for(int i = 0; i < this.tempBuffer1.size(); i++) {',
                 '    this.inputValLookAsideBuffer[this.nVals + i] = this.individualInputValsCount + ((int[])this.tempBuffer1.getArray())[i];',
                 '}',
                 'System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputValIndices, this.individualInputValsCount, this.tempBuffer2.size());',
                 'System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputValVals, this.individualInputValsCount, this.tempBuffer3.size());',
                 'this.individualInputValsCount += this.tempBuffer2.size();' ]
    def getBufferedInit(self):
        return [ ]
    def getResetHelper(self):
        return [ 'List<int[]> accIndices = new ArrayList<int[]>();',
                 'List<float[]> accVals = new ArrayList<float[]>();' ]
    def getAddValHelper(self):
        return [ 'accIndices.add(v.indices());',
                 'accVals.add(v.vals());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLFsvecValueIterator(
                       accIndices, accVals));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        # Can't be a key, so isKey always == False
        if isInput:
            if isMapper:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
            else:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
        else:
            return [ '(outputValIntLookAsideBuffer.length * 4) +',
                     '(outputValFloatLookAsideBuffer.length * 4) +',
                     '(outputValIndices.length * 4) +',
                     '(outputValVals.length * 8) +',
                     '(outputValLengthBuffer.length * 4) +',
                     '(bufferOutputIndices == null ? 0 : bufferOutputIndices.length * 4) +',
                     '(bufferOutputVals == null ? 0 : bufferOutputVals.length * 8) +',
                     '(memAuxIntIncr.length * 4) +',
                     '(memAuxFloatIncr.length * 4);' ]
    def getOutputLength(self, core, count):
        return [ '(this.output'+core+'IntLookAsideBuffer['+count+'-1]+',
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Indices.length+" int memory, and "+',
                 '(this.output'+core+'FloatLookAsideBuffer['+count+'-1]+'
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Vals.length+" float memory"' ]
    def getInputLength(self, core, isMapper):
        return [ 'this.individualInputValsCount+"/"+this.input'+core+'Indices.length+" '+core.lower()+'s elements"' ]

#################################################################################
########################## Visitor for Bsvec type ###############################
#################################################################################
class BsvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel):
        buf = [ ]
        if isInput:
            buf.append('public int[] '+basename+'LookAsideBuffer;')
            buf.append('public int[] '+basename+'Indices;')
            buf.append('public double[] '+basename+'Vals;')
        else:
            buf.append('public int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public int[] '+basename+'DoubleLookAsideBuffer;')
            buf.append('public int[] '+basename+'Indices;')
            buf.append('public double[] '+basename+'Vals;')

        if not isInput:
            buf.append('private int[] bufferOutputIndices = null;')
            buf.append('private double[] bufferOutputVals = null;')
            buf.append('public int[] outputValLengthBuffer;')
        return buf

    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        buf = [ ]
        if forceNull:
            if isInput:
                buf.append(basename+'LookAsideBuffer = null;')
            else:
                buf.append(basename+'IntLookAsideBuffer = null;')
                buf.append(basename+'DoubleLookAsideBuffer = null;')
            buf.append(basename+'Indices = null;')
            buf.append(basename+'Vals = null;')
        else:
            if isInput:
                buf.append(basename+'LookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'DoubleLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new double[this.clContext.getPreallocDoubleLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
            buf.append('this.nVectorsToBuffer = clContext.getNVectorsToBuffer();')
        if not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize() * outputsPerInput];')
            buf.append('memAuxIntIncr = new int[1];')
            buf.append('memAuxDoubleIncr = new int[1];')
        return buf

    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        buf = [ ]
        buf.append('this.arrayLengths.put("'+basename+'IntLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'DoubleLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'Indices", this.clContext.getPreallocIntLength());')
        buf.append('this.arrayLengths.put("'+basename+'Vals", this.clContext.getPreallocDoubleLength());')
        if not isKey:
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize() * this.getOutputPairsPerInput());')
            buf.append('this.arrayLengths.put("memAuxIntIncr", 1);')
            buf.append('this.arrayLengths.put("memAuxDoubleIncr", 1);')

        return buf

    def getMarkerInit(self, varname, size, forceNull):
        if forceNull:
            return [ varname+' = null;' ]
        else:
            return [ varname+' = new int['+size+'];' ]
    def getKeyValSetup(self, basename, isInput, isKey, forceNull):
        buf = [ ]
        if isInput:
            if forceNull:
                buf.append('this.'+basename+'LookAsideBuffer = null;')
            else:
                buf.append('this.'+basename+'LookAsideBuffer = set'+basename.capitalize()+'LookAsideBuffer;')
        else:
            if forceNull:
                buf.append('this.'+basename+'IntLookAsideBuffer = null;')
                buf.append('this.'+basename+'DoubleLookAsideBuffer = null;')
            else:
                buf.append('this.'+basename+'IntLookAsideBuffer = set'+basename.capitalize()+'IntLookAsideBuffer;')
                buf.append('this.'+basename+'DoubleLookAsideBuffer = set'+basename.capitalize()+'DoubleLookAsideBuffer;')

        if forceNull:
            buf.append('this.'+basename+'Indices = null;')
            buf.append('this.'+basename+'Vals = null;')
        else:
            buf.append('this.'+basename+'Indices = set'+basename.capitalize()+'Indices;')
            buf.append('this.'+basename+'Vals = set'+basename.capitalize()+'Vals;')

        if not isKey and not isInput:
            if forceNull:
                buf.append('this.outputValLengthBuffer = null;')
            else:
                buf.append('this.outputValLengthBuffer = setOutputValLengthBuffer;')
        return buf

    def getSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type bsvec')
        return [ 'int[] '+basename+'Indices, double[] '+basename+'Vals, int len' ]
    def getWriteWithOffsetSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type bsvec')
        return [ 'int[] '+basename+'Indices, int indicesOffset, double[] '+basename+'Vals, int valsOffset, int len' ]
    def getWriteMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type bsvec')
        return [ basename+'Obj.set('+basename+'Indices, '+basename+'Vals, len);' ]
    def getWriteWithOffsetMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type bsvec')
        return [ basename+'Obj.set('+basename+'Indices, indicesOffset, '+basename+'Vals, valsOffset, len);' ]
    def getIterArg(self):
        return [ 'HadoopCLSvecValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type bsvec')
        return [ 'input'+basename+'Indices, input'+basename+'Vals, input'+basename+'LookAsideBuffer[3] + this.nPairs + this.individualInputValsCount' ]
    def getKernelCallIter(self):
        return [ 'null' ]
    def getOriginalInitMethod(self):
        return [ 'this.tempBuffer1 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer2 = new HadoopCLResizableIntArray();',
                 'this.tempBuffer3 = new HadoopCLResizableDoubleArray();' ]
    def getSetupParameter(self, basename, isInput, isKey):
        buf = [ ]
        if isInput:
            buf.append('int[] set'+basename.capitalize()+'LookAsideBuffer, ')
        else:
            buf.append('int[] set'+basename.capitalize()+'IntLookAsideBuffer, ')
            buf.append('int[] set'+basename.capitalize()+'DoubleLookAsideBuffer, ')
        buf.append('int[] set'+basename.capitalize()+'Indices, ')
        buf.append('double[] set'+basename.capitalize()+'Vals')
        if not isInput and not isKey:
            buf.append(', int[] setOutputValLengthBuffer')
        return buf
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputValIntLookAsideBuffer");',
                 'this.outputAuxIntLength = this.getArrayLength("outputValIndices");',
                 'this.outputAuxDoubleLength = this.getArrayLength("outputValVals");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputValLookAsideBuffer[this.nPairs] = this.individualInputValsCount;',
                 'if (this.enableStriding) {',
                 '    IndValWrapper wrapper = new IndValWrapper(actual.indices(), actual.vals(), actual.size());',
                 '    if (this.sortedVals.containsKey(actual.size())) {',
                 '        this.sortedVals.get(actual.size()).add(wrapper);',
                 '    } else {',
                 '        LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();',
                 '        newList.add(wrapper);',
                 '        this.sortedVals.put(actual.size(), newList);',
                 '    }',
                 # '    this.inputValIndices.ensureCapacity( (this.nPairs +',
                 # '        ((actual.size() - 1) * nVectorsToBuffer)) + 1);',
                 # '    this.inputValVals.ensureCapacity( (this.nPairs +',
                 # '        ((actual.size() - 1) * nVectorsToBuffer)) + 1);',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputValIndices.unsafeSet(this.nPairs + (i * nVectorsToBuffer),',
                 # '            actual.indices()[i]);',
                 # '        this.inputValVals.unsafeSet(this.nPairs + (i * nVectorsToBuffer),',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '} else {',
                 '    System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());',
                 # '    for (int i = 0; i < actual.size(); i++) {',
                 # '        this.inputValIndices.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.indices()[i]);',
                 # '        this.inputValVals.unsafeSet(this.individualInputValsCount + i,',
                 # '            actual.vals()[i]);',
                 # '    }',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.indices(), 0, this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), 0, this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
#    def getAddKeyMethod(self, index):
#    def getLimitSetter():
#    def getCloneIncompleteMapperKey():
    def getCloneIncompleteMapperValue(self):
        return [ 'newBuffer.inputValLookAsideBuffer[newBuffer.nPairs] = newBuffer.individualInputValsCount;',
                 'int length;',
                 'if (this.enableStriding) {',
                 '    length = (i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1]) - this.inputValLookAsideBuffer[i];',
                 '    for (int j = 0; j < length; j++) {',
                 '        newBuffer.inputValIndices.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputValIndices.get(i + (j * this.nPairs)));',
                 '        newBuffer.inputValVals.unsafeSet(',
                 '            newBuffer.nPairs + (j * nRestarts),',
                 '            this.inputValVals.get(i + (j * this.nPairs)));',
                 '    }',
                 '} else {',
                 '    int baseOffset = this.inputValLookAsideBuffer[i];',
                 '    int topOffset = i == this.nPairs-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[i+1];',
                 '    length = topOffset - baseOffset;',
                 '    System.arraycopy((int[])(this.inputValIndices.getArray()),',
                 '        baseOffset, (int[])(newBuffer.inputValIndices.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '    System.arraycopy((double[])(this.inputValVals.getArray()),',
                 '        baseOffset, (double[])(newBuffer.inputValVals.getArray()),',
                 '        newBuffer.inputValLookAsideBuffer[newBuffer.nPairs], topOffset-baseOffset);',
                 '}',
                 'newBuffer.individualInputValsCount += length;' ]
#    def getCloneIncompleteReducerKey():
    def getCloneIncompleteReducerValue(self):
        return [ 'for(int j = baseValOffset; j < topValOffset; j++) {',
                 '    int offsetInNewBuffer = newBuffer.nVals + j-baseValOffset;',
                 '    newBuffer.inputValLookAsideBuffer[offsetInNewBuffer] = newBuffer.individualInputValsCount;',
                 '    int baseOffset = this.inputValLookAsideBuffer[j];',
                 '    int topOffset = j == this.nVals-1 ? this.individualInputValsCount : this.inputValLookAsideBuffer[j+1];',
                 '    System.arraycopy(this.inputValIndices, baseOffset, newBuffer.inputValIndices, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    System.arraycopy(this.inputValVals, baseOffset, newBuffer.inputValVals, newBuffer.inputValLookAsideBuffer[offsetInNewBuffer], topOffset-baseOffset);',
                 '    newBuffer.individualInputValsCount += (topOffset - baseOffset);',
                 '}' ]
    def getFillParameter(self, basename, isInput, isMapper):
        buf = [ ]
        if isInput:
            buf.append(basename+'LookAsideBuffer, '+basename+'Indices, '+basename+'Vals')
        else:
            buf.append(basename+'IntLookAsideBuffer, '+basename+'DoubleLookAsideBuffer, '+basename+'Indices, '+basename+'Vals')
        return buf
    def getMapArguments(self, varName):
        return [ varName+'.indices(), '+varName+'.vals(), '+varName+'.size()' ]
    def getBufferedDecl(self):
        return [ ]
    def getBufferInputValue(self):
        return [ '((HadoopCLResizableIntArray)this.tempBuffer1).add(this.tempBuffer2.size());',
                 'for(int i = 0; i < actual.size(); i++) {',
                 '    ((HadoopCLResizableIntArray)this.tempBuffer2).add(actual.indices()[i]);',
                 '    ((HadoopCLResizableDoubleArray)this.tempBuffer3).add(actual.vals()[i]);',
                 '}' ]
    def getUseBufferedValues(self):
        return [ 'for(int i = 0; i < this.tempBuffer1.size(); i++) {',
                 '    this.inputValLookAsideBuffer[this.nVals + i] = this.individualInputValsCount + ((int[])this.tempBuffer1.getArray())[i];',
                 '}',
                 'System.arraycopy(this.tempBuffer2.getArray(), 0, this.inputValIndices, this.individualInputValsCount, this.tempBuffer2.size());',
                 'System.arraycopy(this.tempBuffer3.getArray(), 0, this.inputValVals, this.individualInputValsCount, this.tempBuffer3.size());',
                 'this.individualInputValsCount += this.tempBuffer2.size();' ]
    def getBufferedInit(self):
        return [ ]
    def getResetHelper(self):
        return [ 'List<int[]> accIndices = new ArrayList<int[]>();',
                 'List<double[]> accVals = new ArrayList<double[]>();' ]
    def getAddValHelper(self):
        return [ 'accIndices.add(v.indices());',
                 'accVals.add(v.vals());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLSvecValueIterator(
                       accIndices, accVals));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        # Can't be a key, so isKey always == False
        if isInput:
            if isMapper:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
            else:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
        else:
            return [ '(outputValIntLookAsideBuffer.length * 4) +',
                     '(outputValDoubleLookAsideBuffer.length * 4) +',
                     '(outputValIndices.length * 4) +',
                     '(outputValVals.length * 8) +',
                     '(outputValLengthBuffer.length * 4) +',
                     '(bufferOutputIndices == null ? 0 : bufferOutputIndices.length * 4) +',
                     '(bufferOutputVals == null ? 0 : bufferOutputVals.length * 8) +',
                     '(memAuxIntIncr.length * 4) +',
                     '(memAuxDoubleIncr.length * 4);' ]
    def getOutputLength(self, core, count):
        return [ '(this.output'+core+'IntLookAsideBuffer['+count+'-1]+',
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Indices.length+" int memory, and "+',
                 '(this.output'+core+'DoubleLookAsideBuffer['+count+'-1]+'
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Vals.length+" double memory"' ]
    def getInputLength(self, core, isMapper):
        return [ 'this.individualInputValsCount+"/"+this.input'+core+'Indices.length+" '+core.lower()+'s elements"' ]

#################################################################################
########################## End of visitors ######################################
#################################################################################

visitorsMap = { 'int': PrimitiveVisitor('int'),
                'float': PrimitiveVisitor('float'),
                'double': PrimitiveVisitor('double'),
                'long': PrimitiveVisitor('long'),
                'pair': PairVisitor(),
                'ipair': IpairVisitor(),
                'svec': SvecVisitor(),
                'ivec': IvecVisitor(),
                'fsvec': FsvecVisitor(),
                'bsvec': BsvecVisitor() }

def visitor(nativeTyp):
    return visitorsMap[nativeTyp]

#################################################################################
########################## End of visitor getters ###############################
#################################################################################

def checkTypeSupported(type):
    return type in supportedTypes

def kernelString(isMapper):
    if isMapper:
        return 'mapper'
    else:
        return 'reducer'

def capitalizedKernelString(isMapper):
    return kernelString(isMapper).capitalize()

def compactHadoopName(name):
    if name == 'UniquePair':
        return 'UPair'
    elif name == 'SparseVector':
        return 'Svec'
    elif name == 'IntegerVector':
        return 'Ivec'
    elif name == 'FSparseVector':
        return 'Fsvec'
    elif name == 'BSparseVector':
        return 'Bsvec'
    else:
        return name

def isPrimitiveHadoopType(typeName):
    if typeName == 'Pair' or typeName == 'UniquePair' or typeName == 'SparseVector' or \
            typeName == 'IntegerVector' or typeName == 'FSparseVector' or \
            typeName == 'BSparseVector':
        return False
    return True

def typeNameForClassName(t):
    if t == 'ipair':
        return 'UPair'
    else:
        # should work for svec, ivec, etc
        return t.capitalize()

def typeNameForWritable(t):
    if t == 'ipair':
        return 'UniquePair'
    elif t == 'svec':
        return 'SparseVector'
    elif t == 'ivec':
        return 'IntegerVector'
    elif t == 'fsvec':
        return 'FSparseVector'
    elif t == 'bsvec':
        return 'BSparseVector'
    else:
        return t.capitalize()

def isVariableLength(t):
    return t in variablelength

def isNonPrimitive(t):
    return t in nonprimitives or t in variablelength

def isPrimitive(t):
    return t in primitives

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

def inputBufferClassName(isMapper, inputKeyType, inputValueType):
    return typeNameForClassName(inputKeyType)+typeNameForClassName(inputValueType)+ \
        'HadoopCLInput'+('Mapper' if isMapper else 'Reducer')+'Buffer'

def outputBufferClassName(isMapper, inputKeyType, inputValueType):
    return typeNameForClassName(inputKeyType)+typeNameForClassName(inputValueType)+ \
        'HadoopCLOutput'+('Mapper' if isMapper else 'Reducer')+'Buffer'

# def bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType):
#     return classNameHelper(isMapper, inputKeyType, inputValueType, outputKeyType, 
#             outputValueType, 'HadoopCL', 'Buffer')

def generateWriteWithOffsetSig(outputKeyType, outputValType, fp):
    fp.write('\n')
    fp.write('    protected boolean write(')
    write(visitor(outputKeyType).getSig('key', True), 0, fp)
    fp.write(', ')
    write(visitor(outputValType).getWriteWithOffsetSig('val', False), 0, fp)
    fp.write(') {\n')

def generateWriteWithOffsetMethod(fp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType):
    generateWriteWithOffsetSig(nativeOutputKeyType, nativeOutputValueType, fp)
    fp.write('        this.javaProfile.stopKernel();\n')
    fp.write('        this.javaProfile.startWrite();\n')
    writeln(visitor(nativeOutputKeyType).getWriteMethodBody('key', True), 2, fp)
    writeln(visitor(nativeOutputValueType).getWriteWithOffsetMethodBody('val', False), 2, fp)
    fp.write('        try { clContext.getContext().write(keyObj, valObj); } catch(Exception ex) { throw new RuntimeException(ex); }\n')
    fp.write('        this.javaProfile.stopWrite();\n')
    fp.write('        this.javaProfile.startKernel();\n')
    fp.write('        return true;\n')
    fp.write('    }\n')
    fp.write('\n')

def generateWriteSig(outputKeyType, outputValType, fp):
    fp.write('\n')
    fp.write('    protected boolean write(')
    write(visitor(outputKeyType).getSig('key', True), 0, fp)
    fp.write(', ')
    write(visitor(outputValType).getSig('val', False), 0, fp)
    fp.write(') {\n')

def generateWriteMethod(fp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType):
    generateWriteSig(nativeOutputKeyType, nativeOutputValueType, fp)
    fp.write('        this.javaProfile.stopKernel();\n')
    fp.write('        this.javaProfile.startWrite();\n')
    writeln(visitor(nativeOutputKeyType).getWriteMethodBody('key', True), 2, fp)
    writeln(visitor(nativeOutputValueType).getWriteMethodBody('val', False), 2, fp)
    fp.write('        try { clContext.getContext().write(keyObj, valObj); } catch(Exception ex) { throw new RuntimeException(ex); }\n')
    fp.write('        this.javaProfile.stopWrite();\n')
    fp.write('        this.javaProfile.startKernel();\n')
    fp.write('        return true;\n')
    fp.write('    }\n')
    fp.write('\n')

def generateKernelDecl(isMapper, keyType, valType, fp):
    if isMapper:
        fp.write('    protected abstract void map(')
        write(visitor(keyType).getSig('key', True), 0, fp)
        fp.write(', ')
        write(visitor(valType).getSig('val', False), 0, fp)
    else:
        fp.write('    protected abstract void reduce(')
        write(visitor(keyType).getSig('key', True), 0, fp)
        fp.write(', ')
        write(visitor(valType).getIterArg(), 0, fp)
    fp.write(');\n')

def generateKernelCall(isMapper, keyType, valType, fp):
    fp.write('    @Override\n')
    if isMapper:
        fp.write('    protected void callMap() {\n')
        fp.write('        map(')
        write(visitor(keyType).getKernelCall('Key', True), 0, fp)
        fp.write(', ')
        write(visitor(valType).getKernelCall('Val', False), 0, fp)
    else:
        fp.write('    protected void callReduce(int startOffset, int stopOffset)  {\n')
        fp.write('        reduce(')
        write(visitor(keyType).getKernelCall('Key', True), 0, fp)
        fp.write(', ')
        write(visitor(valType).getKernelCallIter(), 0, fp)
    fp.write(');\n')
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
    fp.write('import java.util.concurrent.locks.ReentrantLock;\n')
    fp.write('import java.util.TreeMap;\n')
    fp.write('import java.util.LinkedList;\n')
    fp.write('import java.util.Iterator;\n')
    if isMapper:
        fp.write('import org.apache.hadoop.mapreduce.Mapper.Context;\n')
    else:
        fp.write('import org.apache.hadoop.mapreduce.Reducer.Context;\n')
    fp.write('\n')

# def writePopulatesMethod(fp, nativeOutputKeyType, nativeOutputValueType):
#     fp.write('\n')
#     fp.write('    @Override\n')
#     fp.write('    public void populate(Object genericReducerKeys, Object genericReducerValues, int[] keyIndex) {\n')
#     if isNonPrimitive(nativeOutputValueType):
#         fp.write('        throw new RuntimeException("Invalid to call populate on mapper with non-primitive output");\n')
#     else:
#         fp.write('        int[] privateKeyIndex = new int[keyIndex.length];\n')
#         fp.write('        System.arraycopy(keyIndex, 0, privateKeyIndex, 0, keyIndex.length);\n')
#         fp.write('        '+nativeOutputKeyType+'[] reducerKeys = ('+nativeOutputKeyType+'[])genericReducerKeys;\n')
#         fp.write('        '+nativeOutputValueType+'[] reducerValues = ('+nativeOutputValueType+'[])genericReducerValues;\n')
#         fp.write('\n')
#         
#         firstLines = [ ]
#         firstLines.append('                          int index = 0;\n')
#         firstLines.append('                          while(reducerKeys[index] != this.outputKeys[DUMMY]) index++;\n')
#         firstLines.append('                          reducerValues[privateKeyIndex[index]++] = this.outputVals[DUMMY];\n')
#         
#         secondLines = [ ]
#         secondLines.append('                          int index = 0;\n')
#         secondLines.append('                          while(reducerKeys[index] != this.outputKeys[DUMMY]) index++;\n')
#         secondLines.append('                          reducerValues[privateKeyIndex[index]++] = this.outputVals[DUMMY];\n')
#         
#         writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLines, secondLines)
#         
#     fp.write('    }\n')

# def writeKeyCountsMethod(fp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType):
#     fp.write('\n')
#     fp.write('    @Override\n')
#     fp.write('    public HashMap getKeyCounts() {\n')
#     if isNonPrimitive(nativeOutputValueType):
#         fp.write('        throw new RuntimeException("Invalid to call getKeyCounts on mapper with non-primitive output");\n')
#     else:
#         fp.write('        HashMap<'+hadoopOutputKeyType+'Writable, HadoopCLMutableInteger> keyCounts = new HashMap<'+hadoopOutputKeyType+'Writable, HadoopCLMutableInteger>();\n')
#         
#         firstLines = [ ]
#         firstLines.append('                           '+hadoopOutputKeyType+'Writable temp = new '+hadoopOutputKeyType+'Writable(this.outputKeys[DUMMY]);\n')
#         firstLines.append('                           if(!keyCounts.containsKey(temp)) keyCounts.put(temp, new HadoopCLMutableInteger());\n')
#         firstLines.append('                           keyCounts.get(temp).incr();\n')
#         
#         secondLines = [ ]
#         secondLines.append('                               '+hadoopOutputKeyType+'Writable temp = new '+hadoopOutputKeyType+'Writable(this.outputKeys[DUMMY]);\n')
#         secondLines.append('                               if(!keyCounts.containsKey(temp)) keyCounts.put(temp, new HadoopCLMutableInteger());\n')
#         secondLines.append('                               keyCounts.get(temp).incr();\n')
#         
#         writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLines, secondLines)
#         
#         fp.write('        return keyCounts;\n')
#     fp.write('    }\n')

def writeOutputBufferInit(isMapper, fp, nativeOutputKeyType, nativeOutputValueType):
    writeln(visitor(nativeOutputKeyType).getMarkerInit(
        'outputIterMarkers', 'this.clContext.getOutputBufferSize() * outputsPerInput',
            False), 3, fp)
    writeln(visitor(nativeOutputKeyType).getKeyValInit('outputKey',
        'this.clContext.getOutputBufferSize() * outputsPerInput', False, False,
            isMapper, True), 3, fp)
    writeln(visitor(nativeOutputValueType).getKeyValInit('outputVal',
        'this.clContext.getOutputBufferSize() * outputsPerInput', False, False,
            isMapper, False), 3, fp)

def writeOriginalInputInitMethod(fp, nativeInputKeyType, nativeInputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void init(int outputsPerInput, HadoopOpenCLContext clContext) {\n')
    fp.write('        baseInit(clContext);\n')
    fp.write('\n')
    # if not isMapper:
    #     writeln(visitor(nativeInputValueType).getOriginalInitMethod(), 2, fp)
    #     fp.write('\n')

    writeln(visitor(nativeInputKeyType).getKeyValInit('inputKey',
        'this.clContext.getInputBufferSize()', False, True, isMapper, True), 2, fp)

    if isMapper:
        writeln(visitor(nativeInputValueType).getKeyValInit('inputVal',
            'this.clContext.getInputBufferSize()', False, True, isMapper, False), 2, fp)
    else:
        writeln(visitor(nativeInputValueType).getKeyValInit('inputVal',
            'this.clContext.getInputBufferSize()',
                False, True, isMapper, False), 2, fp)

    fp.write('        this.initialized = true;\n')
    fp.write('    }\n')
    fp.write('\n')

def writeInitBeforeKernelMethod(fp, isMapper, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void initBeforeKernel(int outputsPerInput, HadoopOpenCLContext clContext) {\n')
    fp.write('        baseInit(clContext);\n')
    fp.write('        this.outputsPerInput = outputsPerInput;\n')
    fp.write('\n')
    writeOutputBufferInit(isMapper, fp, nativeOutputKeyType, nativeOutputValueType)
    fp.write('        this.initialized = true;\n')
    fp.write('    }\n')
    fp.write('\n')

def writePostKernelSetupDeclaration(fp, isMapper, nativeOutputKeyType, nativeOutputValueType):
    fp.write('    public void postKernelSetup(')
    write(visitor(nativeOutputKeyType).getSetupParameter('outputKey', False, True), 0, fp)
    fp.write(', ')
    write(visitor(nativeOutputValueType).getSetupParameter('outputVal', False, False), 0, fp)

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec':
        fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxDoubleIncr')
    elif nativeOutputValueType == 'ivec':
        fp.write(', int[] setMemAuxIncr')
    elif nativeOutputValueType == 'fsvec':
        fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxFloatIncr')

    fp.write(', int[] setMemIncr, int[] setMemRetry, int[] setNWrites, int setOutputsPerInput, int[] outputIterMarkers) {\n')


def writePostKernelSetupMethod(fp, isMapper, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    writePostKernelSetupDeclaration(fp, isMapper, nativeOutputKeyType, nativeOutputValueType)

    writeln(visitor(nativeOutputKeyType).getKeyValSetup('outputKey', False, True, False), 2, fp)
    writeln(visitor(nativeOutputValueType).getKeyValSetup('outputVal', False, False, False), 2, fp)

    fp.write('\n')
    fp.write('        this.memIncr = setMemIncr;\n')
    fp.write('        this.memRetry = setMemRetry;\n')
    fp.write('        this.nWrites = setNWrites;\n')
    # fp.write('        this.memIncr[0] = 0;\n')
    fp.write('\n')

    # writeln(visitor(nativeOutputValueType).getSetLengths(), 2, fp)
    fp.write('        this.outputIterMarkers = outputIterMarkers;\n')

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec':
        fp.write('        this.memAuxIntIncr = setMemAuxIntIncr;\n')
        fp.write('        this.memAuxDoubleIncr = setMemAuxDoubleIncr;\n')
        fp.write('        this.memAuxIntIncr[0] = 0;\n')
        fp.write('        this.memAuxDoubleIncr[0] = 0;\n')
        fp.write('\n')
    elif nativeOutputValueType == 'ivec':
        fp.write('        this.memAuxIncr = setMemAuxIncr;\n')
        fp.write('        this.memAuxIncr[0] = 0;\n')
    elif nativeOutputValueType == 'fsvec':
        fp.write('        this.memAuxIntIncr = setMemAuxIntIncr;\n')
        fp.write('        this.memAuxFloatIncr = setMemAuxFloatIncr;\n')
        fp.write('        this.memAuxIntIncr[0] = 0;\n')
        fp.write('        this.memAuxFloatIncr[0] = 0;\n')
        fp.write('\n')

    fp.write('    }\n')


def writePreKernelSetupDeclaration(fp, isMapper, nativeInputKeyType, nativeInputValueType):
    fp.write('    public void preKernelSetup(')
    write(visitor(nativeInputKeyType).getSetupParameter('inputKey', True, True), 0, fp)
    fp.write(', ')
    write(visitor(nativeInputValueType).getSetupParameter('inputVal', True, False), 0, fp)
    # fp.write(', ')
    # write(visitor(nativeOutputKeyType).getSetupParameter('outputKey', False, True), 0, fp)
    # fp.write(', ')
    # write(visitor(nativeOutputValueType).getSetupParameter('outputVal', False, False), 0, fp)

    if isMapper:
        fp.write(', int[] setNWrites, int setNPairs')
    else:
        fp.write(', int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals')

    if isVariableLength(nativeInputValueType):
        fp.write(', int setIndividualInputValsCount')

    # if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec':
    #     fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxDoubleIncr')
    # elif nativeOutputValueType == 'ivec':
    #     fp.write(', int[] setMemAuxIncr')
    # elif nativeOutputValueType == 'fsvec':
    #     fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxFloatIncr')

    # fp.write(', int[] setMemIncr, int setOutputsPerInput, int[] outputIterMarkers) {\n')
    fp.write(', int setOutputsPerInput) {\n')

def writePreKernelSetupMethod(fp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    writePreKernelSetupDeclaration(fp, isMapper, nativeInputKeyType, nativeInputValueType)

    writeln(visitor(nativeInputKeyType).getKeyValSetup('inputKey', True, True, False), 2, fp)
    writeln(visitor(nativeInputValueType).getKeyValSetup('inputVal', True, False, False), 2, fp)
    writeln(visitor(nativeOutputKeyType).getKeyValSetup('outputKey', False, True, True), 2, fp)
    writeln(visitor(nativeOutputValueType).getKeyValSetup('outputVal', False, False, True), 2, fp)

    if isMapper:
        fp.write('        this.nWrites = setNWrites;\n')
        fp.write('        this.nPairs = setNPairs;\n')
    else:
        fp.write('        this.input_keyIndex = setKeyIndex;\n')
        fp.write('        this.nWrites = setNWrites;\n')
        fp.write('        this.nKeys = setNKeys;\n')
        fp.write('        this.nVals = setNVals;\n')

    fp.write('\n')
    fp.write('        this.memIncr = null;\n')
    fp.write('        this.memRetry = null;\n')
    fp.write('        this.outputsPerInput = setOutputsPerInput;\n')
    fp.write('\n')

    writeln(visitor(nativeOutputValueType).getSetLengths(), 2, fp)
    fp.write('        this.outputIterMarkers = null;\n')

    if isVariableLength(nativeInputValueType):
        fp.write('        this.individualInputValsCount = setIndividualInputValsCount;\n')
        fp.write('\n')

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec':
        fp.write('        this.memAuxIntIncr = null;\n')
        fp.write('        this.memAuxDoubleIncr = null;\n')
        fp.write('\n')
    elif nativeOutputValueType == 'ivec':
        fp.write('        this.memAuxIncr = null;\n')
    elif nativeOutputValueType == 'fsvec':
        fp.write('        this.memAuxIntIncr = null;\n')
        fp.write('        this.memAuxFloatIncr = null;\n')
        fp.write('\n')

    if profileMemoryUtilization:
        fp.write('        System.out.println("'+('Mapper' if isMapper else 'Reducer')+': Input using "+\n')
        write_without_last_ln(visitor(nativeInputKeyType).getInputLength('Key', isMapper), 3, fp)
        fp.write('+", "+\n')
        write_without_last_ln(visitor(nativeInputValueType).getInputLength('Val', isMapper), 3, fp)
        fp.write(');\n')
        fp.write('\n')

    fp.write('    }\n')

def writeInitMethod(fp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public void init(HadoopOpenCLContext clContext) {\n')
    fp.write('        baseInit(clContext);\n')
    # Just init to false, it gets actually set in fill
    fp.write('        this.setStrided(false);\n')
    fp.write('\n')
    fp.write('        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize() * this.getOutputPairsPerInput());\n')
    fp.write('        this.arrayLengths.put("memIncr", 1);\n')
    fp.write('        this.arrayLengths.put("memRetry", 1);\n')
    writeln(visitor(nativeOutputKeyType).getArrayLengthInit('outputKey',
        'this.clContext.getOutputBufferSize() * this.getOutputPairsPerInput()',
            isMapper, True), 2, fp)
    writeln(visitor(nativeOutputValueType).getArrayLengthInit('outputVal',
        'this.clContext.getOutputBufferSize() * this.getOutputPairsPerInput()',
            isMapper, False), 2, fp)
    fp.write('    }\n')
    fp.write('\n')

def writeAddValueMethod(fp, hadoopInputValueType, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public void addTypedValue(Object val) {\n')
    fp.write('        '+hadoopInputValueType+'Writable actual = ('+hadoopInputValueType+'Writable)val;\n')

    if isMapper:
        writeln(visitor(nativeInputValueType).getAddValueMethodMapper(), 2, fp)
    else:
        writeln(visitor(nativeInputValueType).getAddValueMethodReducer(), 2, fp)

    fp.write('    }\n')
    fp.write('\n')

def writeAddKeyMethod(fp, hadoopInputKeyType, nativeInputKeyType):
    fp.write('    @Override\n')
    fp.write('    public void addTypedKey(Object key) {\n')
    fp.write('        '+hadoopInputKeyType+'Writable actual = ('+hadoopInputKeyType+'Writable)key;\n')
    if isMapper:
        writeln(visitor(nativeInputKeyType).getAddKeyMethod('this.nPairs'), 2, fp)
    else:
        fp.write('        if (this.currentKey == null || !this.currentKey.equals(actual)) {\n')
        fp.write('            this.keyIndex[this.nKeys] = this.nVals;\n')
        writeln(visitor(nativeInputKeyType).getAddKeyMethod('this.nKeys'), 3, fp)
        fp.write('            this.nKeys++;\n')
        fp.write('            this.currentKey = actual.clone();\n')
        fp.write('        }\n')
    fp.write('    }\n')
    fp.write('\n')

# def writeTransferBufferedValues(fp, isMapper):
#     fp.write('    @Override\n')
#     fp.write('    public void transferBufferedValues(HadoopCLBuffer buffer) {\n')
#     if isMapper:
#         fp.write('        // NOOP\n')
#     else:
#         fp.write('        this.tempBuffer1.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer1);\n')
#         fp.write('        if(this.tempBuffer2 != null) this.tempBuffer2.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer2);\n')
#         fp.write('        if(this.tempBuffer3 != null) this.tempBuffer3.copyTo(((HadoopCLInputReducerBuffer)buffer).tempBuffer3);\n')
#     fp.write('    }\n')

def writeResetMethod(fp, isMapper, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public void reset() {\n')
    if isMapper:
        fp.write('        this.nPairs = 0;\n')
        if nativeInputValueType == 'svec' or nativeInputValueType == 'bsvec':
            fp.write('        this.individualInputValsCount = 0;\n')
            fp.write('        this.sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')
        elif nativeInputValueType == 'ivec':
            fp.write('        this.individualInputValsCount = 0;\n')
            fp.write('        this.sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')
        elif nativeInputValueType == 'fsvec':
            fp.write('        this.individualInputValsCount = 0;\n')
            fp.write('        this.sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')
    else:
        fp.write('        this.nKeys = 0;\n')
        fp.write('        this.nVals = 0;\n')
        if isVariableLength(nativeInputValueType):
            fp.write('        this.individualInputValsCount = 0;\n')
        fp.write('        this.currentKey = null;\n')

    fp.write('    }\n')
    fp.write('\n')

def writeIsFullMethod(fp, isMapper, nativeInputKeyType, nativeInputValueType, hadoopInputValueType):
    fp.write('    @Override\n')
    fp.write('    public boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {\n')
    if isMapper:
        if nativeInputValueType == 'svec':
            fp.write('        SparseVectorWritable curr = (SparseVectorWritable)((Context)context).getCurrentValue();\n')
            fp.write('        if (this.enableStriding) {\n')
            fp.write('            return this.nPairs == this.capacity() || this.nPairs == nVectorsToBuffer;\n')
            fp.write('        } else {\n')
            fp.write('            return this.nPairs == this.capacity() || this.individualInputValsCount + curr.size() > this.inputValIndices.length;\n')
            fp.write('        }\n')
        elif nativeInputValueType == 'bsvec':
            fp.write('        BSparseVectorWritable curr = (BSparseVectorWritable)((Context)context).getCurrentValue();\n')
            fp.write('        if (this.enableStriding) {\n')
            fp.write('            return this.nPairs == this.capacity() || this.nPairs == nVectorsToBuffer;\n')
            fp.write('        } else {\n')
            fp.write('            return this.nPairs == this.capacity() || this.individualInputValsCount + curr.size() > this.inputValIndices.length;\n')
            fp.write('        }\n')
        elif nativeInputValueType == 'ivec':
            fp.write('        IntegerVectorWritable curr = (IntegerVectorWritable)((Context)context).getCurrentValue();\n')
            fp.write('        if (this.enableStriding) {\n')
            fp.write('            return this.nPairs == this.capacity() || this.nPairs == nVectorsToBuffer;\n')
            fp.write('        } else {\n')
            fp.write('            return this.nPairs == this.capacity() || this.individualInputValsCount + curr.size() > this.inputVal.length;\n')
            fp.write('        }\n')
        elif nativeInputValueType == 'fsvec':
            fp.write('        FSparseVectorWritable curr = (FSparseVectorWritable)((Context)context).getCurrentValue();\n')
            fp.write('        if (this.enableStriding) {\n')
            fp.write('            return this.nPairs == this.capacity() || this.nPairs == nVectorsToBuffer;\n')
            fp.write('        } else {\n')
            fp.write('            return this.nPairs == this.capacity() || this.individualInputValsCount + curr.size() > this.inputValIndices.length;\n')
            fp.write('        }\n')

        else:
            fp.write('        return this.nPairs == this.capacity();\n')
    else:
        fp.write('        Context reduceContext = (Context)context;\n')
        # fp.write('        tempBuffer1.reset();\n')
        # fp.write('        if(tempBuffer2 != null) tempBuffer2.reset();\n')
        # fp.write('        if(tempBuffer3 != null) tempBuffer3.reset();\n')
        # fp.write('        for(Object v : reduceContext.getValues()) {\n')
        # fp.write('            bufferInputValue(v);\n')
        # fp.write('        }\n')
        if nativeInputKeyType == 'pair' or nativeInputKeyType == 'ipair':
          keysName = 'inputKeys1'
        else:
          keysName = 'inputKeys'
        if isVariableLength(nativeInputValueType):
          fp.write('        '+hadoopInputValueType+'Writable curr = ('+hadoopInputValueType+'Writable)reduceContext.getCurrentValue();\n')
          fp.write('        return (this.nKeys == this.'+keysName+'.length || this.individualInputValsCount + curr.size() > this.inputValIndices.length);\n')
        elif nativeInputValueType == 'pair' or nativeInputValueType == 'ipair':
          fp.write('        return (this.nKeys == this.'+keysName+'.length || this.nVals == this.inputVals1.length);\n')
        else:
          fp.write('        return (this.nKeys == this.'+keysName+'.length || this.nVals == this.inputVals.length);\n')

    fp.write('    }\n')
    fp.write('\n')
   
def writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLoopLines, secondLoopLines):
    # Hack to avoid getting RuntimeException in our face, just default
    # to old behavior where we block on the SpillThread. This isn't
    # optimal for performance, but should be better than previously. Plus,
    # this isn't an issue if we don't have combiners and I think Pi is the
    # only existing primitives benchmark that uses combiners...
    # fp.write('            context.setUsingOpenCL(false);\n')
    fp.write('            if (this.memIncr[0] != 0) {\n')
    writeln(visitor(nativeOutputKeyType).getLimitSetter(), 4, fp)
    fp.write('               for(int i = soFar; i < limit; i++) {\n')
    fp.write('                   if (!this.itersFinished.contains(this.outputIterMarkers[i])) continue;\n')

    for line in firstLoopLines:
        fp.write(line.replace('DUMMY', 'i'))

    fp.write('               }\n')
    fp.write('            } else {\n')
    fp.write('               if(isGPU == 0) {\n')
    if isMapper:
        fp.write('                   for(int i = soFar; i < this.nPairs; i++) {\n')
    else:
        fp.write('                   for(int i = soFar; i < this.nKeys; i++) {\n')
    fp.write('                       if (!this.itersFinished.contains(this.outputIterMarkers[i])) continue;\n')
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
        fp.write('                       for(int i = soFar; i < this.nPairs; i++) {\n')
    else:
        fp.write('                       for(int i = soFar; i < this.nKeys; i++) {\n')
    fp.write('                           if (!this.itersFinished.contains(this.outputIterMarkers[i])) continue;\n')
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
    # fp.write('            context.setUsingOpenCL(true);\n')
    fp.write('            return -1;\n')

def writeToHadoopMethod(fp, isMapper, hadoopOutputKeyType, hadoopOutputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar) throws IOException, InterruptedException {\n')
    fp.write('        final '+hadoopOutputKeyType+'Writable saveKey = new '+hadoopOutputKeyType+'Writable();\n')
    fp.write('        final '+hadoopOutputValueType+'Writable saveVal = new '+hadoopOutputValueType+'Writable();\n')
    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec':
        fp.write('        int count;\n')
        fp.write('        if(this.memIncr[0] < 0 || this.outputValIntLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValIntLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
        if profileMemoryUtilization:
          fp.write('        System.out.println("'+('Mapper' if isMapper else 'Reducer')+': Output using "+\n')
          write_without_last_ln(visitor(nativeOutputKeyType).getOutputLength('Key', 'count'), 3, fp)
          fp.write('+", "+\n')
          write_without_last_ln(visitor(nativeOutputValueType).getOutputLength('Val', 'count'), 3, fp)
          fp.write(');\n')
          fp.write('\n')
        fp.write('        for (int i = soFar; i < count; i++) {\n')
        fp.write('            if (!this.itersFinished.contains(this.outputIterMarkers[i])) continue;\n')
        fp.write('            int intStartOffset = this.outputValIntLookAsideBuffer[i];\n')
        fp.write('            int doubleStartOffset = this.outputValDoubleLookAsideBuffer[i];\n')
        fp.write('            int length = this.outputValLengthBuffer[i];\n')
        writeln(visitor(nativeOutputKeyType).getKeyValSet('Key', 'i'), 3, fp)
        fp.write('            saveVal.set(this.outputValIndices, intStartOffset, outputValVals, doubleStartOffset, length);\n')
        fp.write('            try {\n')
        fp.write('                context.write(saveKey, saveVal);\n')
        fp.write('            } catch (DontBlockOnSpillDoneException e) { return i; }\n')
        fp.write('        }\n')
        fp.write('        return -1;\n')
    elif nativeOutputValueType == 'ivec':
        fp.write('        int count;\n')
        fp.write('        if(this.memIncr[0] < 0 || this.outputValLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
        fp.write('              for (int i = soFar; i < count; i++) {\n')
        fp.write('                if (!this.itersFinished.contains(this.outputIterMarkers[i])) continue;\n')
        fp.write('                int startOffset = this.outputValLookAsideBuffer[i];\n')
        fp.write('                int length = this.outputValLengthBuffer[i];\n')
        writeln(visitor(nativeOutputKeyType).getKeyValSet('Key', 'i'), 4, fp)
        fp.write('                saveVal.set(this.outputVal, startOffset, length);\n')
        fp.write('                try {\n')
        fp.write('                    context.write(saveKey, saveVal);\n')
        fp.write('                } catch (DontBlockOnSpillDoneException e) { return i; }\n')
        fp.write('            }\n')
        fp.write('        return -1;\n')
    elif nativeOutputValueType == 'fsvec':
        fp.write('        int count;\n')
        fp.write('        if(this.memIncr[0] < 0 || this.outputValIntLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValIntLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
        fp.write('              for (int i = soFar; i < count; i++) {\n')
        fp.write('                if (!this.itersFinished.contains(this.outputIterMarkers[i])) continue;\n')
        fp.write('                int intStartOffset = this.outputValIntLookAsideBuffer[i];\n')
        fp.write('                int floatStartOffset = this.outputValFloatLookAsideBuffer[i];\n')
        fp.write('                int length = this.outputValLengthBuffer[i];\n')
        writeln(visitor(nativeOutputKeyType).getKeyValSet('Key', 'i'), 4, fp)
        fp.write('                saveVal.set(this.outputValIndices, intStartOffset, outputValVals, floatStartOffset, length);\n')
        fp.write('                try {\n')
        fp.write('                    context.write(saveKey, saveVal);\n')
        fp.write('                } catch (DontBlockOnSpillDoneException e) { return i; }\n')
        fp.write('            }\n')
        fp.write('        return -1;\n')
    else:
        firstLines = [ ]
        firstLines.append(tostr(visitor(nativeOutputKeyType).getKeyValSet('Key', 'DUMMY'), 8))
        firstLines.append(tostr(visitor(nativeOutputValueType).getKeyValSet('Val', 'DUMMY'), 8))
        firstLines.append('                            try {\n')
        firstLines.append('                                context.write(saveKey, saveVal);\n')
        firstLines.append('                            } catch (DontBlockOnSpillDoneException e) { return i; }\n')

        secondLines = [ ]
        secondLines.append(tostr(visitor(nativeOutputKeyType).getKeyValSet('Key', 'DUMMY'), 8))
        secondLines.append(tostr(visitor(nativeOutputValueType).getKeyValSet('Val', 'DUMMY'), 8))
        secondLines.append('                                    try {\n')
        secondLines.append('                                        context.write(saveKey, saveVal);\n')
        secondLines.append('                                    } catch (DontBlockOnSpillDoneException e) { return i; }\n')

        writeToHadoopLoop(fp, nativeOutputKeyType, nativeOutputValueType, firstLines, secondLines)

    fp.write('    }\n')

def generatePrepareForRead(fp, isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType):
    outputBufferClass = outputBufferClassName(isMapper, nativeOutputKeyType, nativeOutputValType)
    fp.write('    @Override\n')
    fp.write('    public void prepareForRead(HadoopCLOutputBuffer genericOutputBuffer) {\n')
    fp.write('        '+outputBufferClass+' outputBuffer = ('+outputBufferClass+')genericOutputBuffer;\n')
    fp.write('        this.postKernelSetup(')
    write(visitor(nativeOutputKeyType).getFillParameter('outputBuffer.outputKey', False, isMapper), 0, fp)
    fp.write(', ')
    write(visitor(nativeOutputValType).getFillParameter('outputBuffer.outputVal', False, isMapper), 0, fp)
    if isVariableLength(nativeOutputValType):
        fp.write(', outputBuffer.outputValLengthBuffer')

    if nativeOutputValType == 'svec' or nativeOutputValType == 'bsvec':
        fp.write(', outputBuffer.memAuxIntIncr, outputBuffer.memAuxDoubleIncr')
    elif nativeOutputValType == 'ivec':
        fp.write(', outputBuffer.memAuxIncr')
    elif nativeOutputValType == 'fsvec':
        fp.write(', outputBuffer.memAuxIntIncr, outputBuffer.memAuxFloatIncr')

    fp.write(', outputBuffer.memIncr, outputBuffer.memRetry, outputBuffer.nWrites, this.outputsPerInput, outputBuffer.outputIterMarkers);\n')
    fp.write('    }\n')
    fp.write('\n')

def generateFill(fp, isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType):
    inputBufferClass = inputBufferClassName(isMapper, nativeInputKeyType, nativeInputValType)
    fp.write('    @Override\n')
    fp.write('    public void fill(HadoopCLInputBuffer genericInputBuffer) {\n')
    fp.write('        '+inputBufferClass+' inputBuffer = ('+inputBufferClass+')genericInputBuffer;\n')

    if isMapper and isVariableLength(nativeInputValType):
        fp.write('        this.setStrided(inputBuffer.enableStriding);\n')
        fp.write('\n')
        fp.write('        if (inputBuffer.enableStriding) {\n')
        fp.write('            int index = 0;\n')
        fp.write('            Iterator<Integer> lengthIter = inputBuffer.sortedVals.descendingKeySet().iterator();\n')
        fp.write('            while (lengthIter.hasNext()) {\n')
        fp.write('                LinkedList<IndValWrapper> pairs = inputBuffer.sortedVals.get(lengthIter.next());\n')
        fp.write('                Iterator<IndValWrapper> pairsIter = pairs.iterator();\n')
        fp.write('                while (pairsIter.hasNext()) {\n')
        fp.write('                    IndValWrapper curr = pairsIter.next();\n')
        fp.write('                    inputBuffer.inputValLookAsideBuffer[index] = inputBuffer.individualInputValsCount;\n')
        if nativeInputValType == 'svec' or nativeInputValType == 'fsvec' or nativeInputValType == 'bsvec':
            fp.write('                    inputBuffer.inputValIndices = ensureCapacity(inputBuffer.inputValIndices, (index + ((curr.length - 1) * inputBuffer.nPairs)) + 1);\n')
            fp.write('                    inputBuffer.inputValVals = ensureCapacity(inputBuffer.inputValVals, (index + ((curr.length - 1) * inputBuffer.nPairs)) + 1);\n')
        else:
            fp.write('                    inputBuffer.inputVal = ensureCapacity(inputBuffer.inputVal, (index + ((curr.length - 1) * inputBuffer.nPairs)) + 1);\n')
        fp.write('                    for (int i = 0; i < curr.length; i++) {\n')
        if nativeInputValType == 'svec' or nativeInputValType == 'fsvec' or nativeInputValType == 'bsvec':
            fp.write('                        inputBuffer.inputValIndices[index + (i * inputBuffer.nPairs)] = curr.indices[i];\n')
            prefix = 'd' if (nativeInputValType == 'svec' or nativeInputValType == 'bsvec') else 'f'
            fp.write('                        inputBuffer.inputValVals[index + (i * inputBuffer.nPairs)] = curr.'+prefix+'vals[i];\n')
        else:
            fp.write('                        inputBuffer.inputVal[index + (i * inputBuffer.nPairs)] = curr.indices[i];\n')
        fp.write('                    }\n')
        fp.write('                    inputBuffer.individualInputValsCount += curr.length;\n')
        fp.write('                    index++;\n')
        fp.write('                } // while (pairsIter)\n')
        fp.write('            } // while (lengthIter)\n')
        fp.write('        } // if (enableStriding)\n')
        fp.write('\n')

    # if not isMapper:
    #     fp.write('        if(this.outputsPerInput < 0 && (outputBuffer.outputKeys == null || outputBuffer.outputKeys.length < inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey)) {\n')
    #     writeln(visitor(nativeOutputKeyType).getMarkerInit(
    #         'outputBuffer.outputIterMarkers', 'inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey', False),
    #         3, fp)
    #     writeln(visitor(nativeOutputKeyType).getKeyValInit('outputBuffer.outputKey',
    #         'inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey', False, False, isMapper, True),
    #         3, fp)
    #     writeln(visitor(nativeOutputValType).getKeyValInit('outputBuffer.outputVal',
    #         'inputBuffer.nKeys * inputBuffer.maxInputValsPerInputKey', False, False, isMapper, False),
    #         3, fp)
    #     fp.write('        }\n')
    fp.write('        this.preKernelSetup(')

    write(visitor(nativeInputKeyType).getFillParameter('inputBuffer.inputKey', True, isMapper), 0, fp)
    fp.write(', ')
    write(visitor(nativeInputValType).getFillParameter('inputBuffer.inputVal', True, isMapper), 0, fp)
    # fp.write(', ')
    # write(visitor(nativeOutputKeyType).getFillParameter('outputBuffer.outputKey', False, isMapper), 0, fp)
    # fp.write(', ')
    # write(visitor(nativeOutputValType).getFillParameter('outputBuffer.outputVal', False, isMapper), 0, fp)

    # if isVariableLength(nativeOutputValType):
    #     fp.write(', outputBuffer.outputValLengthBuffer')

    if isMapper:
        fp.write(', inputBuffer.nWrites, inputBuffer.nPairs')
    else:
        fp.write(', inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals')

    if isVariableLength(nativeInputValType):
        fp.write(', inputBuffer.individualInputValsCount')

    # if nativeOutputValType == 'svec' or nativeOutputValType == 'bsvec':
    #     fp.write(', outputBuffer.memAuxIntIncr, outputBuffer.memAuxDoubleIncr')
    # elif nativeOutputValType == 'ivec':
    #     fp.write(', outputBuffer.memAuxIncr')
    # elif nativeOutputValType == 'fsvec':
    #     fp.write(', outputBuffer.memAuxIntIncr, outputBuffer.memAuxFloatIncr')

    # fp.write(', outputBuffer.memIncr, this.outputsPerInput, outputBuffer.outputIterMarkers);\n')
    fp.write(', this.outputsPerInput);\n')
    fp.write('    }\n')
    fp.write('\n')

# def writeResetForAnotherAttempt(fp, isMapper, nativeInputKeyType, nativeInputValueType):
#     fp.write('    @Override\n')
#     fp.write('    public void resetForAnotherAttempt() {\n')
#     fp.write('        // NO-OP at the moment, but might be necessary later\n')
#     fp.write('    }\n\n')

def writeSpace(fp, isMapper, keyType, valType, isInput):
    fp.write('    @Override\n')
    fp.write('    public long space() {\n')
    fp.write('        return super.space() + \n')
    writeln(visitor(keyType).getSpace(isMapper, isInput, True), 3, fp)
    writeln(visitor(valType).getSpace(isMapper, isInput, False), 3, fp)
    fp.write('    }\n')
    fp.write('\n')


def generateFile(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType):
    nativeInputKeyType = inputKeyType
    nativeInputValueType = inputValueType
    nativeOutputKeyType = outputKeyType
    nativeOutputValueType = outputValueType

    hadoopInputKeyType = typeNameForWritable(inputKeyType)
    hadoopInputValueType = typeNameForWritable(inputValueType)
    hadoopOutputKeyType = typeNameForWritable(outputKeyType)
    hadoopOutputValueType = typeNameForWritable(outputValueType)

    input_fp = open('mapred/org/apache/hadoop/mapreduce/'+inputBufferClassName(isMapper, inputKeyType, inputValueType)+'.java', 'w')
    output_fp = open('mapred/org/apache/hadoop/mapreduce/'+outputBufferClassName(isMapper, outputKeyType, outputValueType)+'.java', 'w')

    # bufferfp = open('mapred/org/apache/hadoop/mapreduce/'+
    #     bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+'.java', 'w')

    kernelfp = open('mapred/org/apache/hadoop/mapreduce/'+
        kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+'.java', 'w')

    writeHeader(input_fp, isMapper);
    writeHeader(output_fp, isMapper);
    writeHeader(kernelfp, isMapper);

    input_fp.write('public class '+
            inputBufferClassName(isMapper, inputKeyType, inputValueType)+' extends HadoopCLInput'+
            capitalizedKernelString(isMapper)+'Buffer {\n')
    output_fp.write('public class '+
            outputBufferClassName(isMapper, outputKeyType, outputValueType)+' extends HadoopCLOutput'+
            capitalizedKernelString(isMapper)+'Buffer {\n')
    # bufferfp.write('public class '+
    #     bufferClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+' extends HadoopCL'+
    #     capitalizedKernelString(isMapper)+'Buffer {\n')

    kernelfp.write('public abstract class '+
        kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+' extends HadoopCL'+
        capitalizedKernelString(isMapper)+'Kernel {\n')

    writeln(visitor(nativeInputKeyType).getKeyValDecl('inputKey', isMapper, True, False), 1, input_fp)
    writeln(visitor(nativeInputValueType).getKeyValDecl('inputVal', isMapper, True, False), 1, input_fp)
    writeln(visitor(nativeOutputKeyType).getKeyValDecl('outputKey', isMapper, False, False), 1, output_fp)
    writeln(visitor(nativeOutputValueType).getKeyValDecl('outputVal', isMapper, False, False), 1, output_fp)

    input_fp.write('    protected int outputsPerInput;\n')
    output_fp.write('    protected int outputsPerInput;\n')
    kernelfp.write('    protected int outputLength;\n')

    if not isMapper:
        writeln(visitor(nativeInputValueType).getBufferedDecl(), 1, kernelfp)
        input_fp.write('    private '+hadoopInputKeyType+'Writable currentKey;\n')

    kernelfp.write('\n')
    if nativeInputValueType == 'svec' or nativeInputValueType == 'fsvec' or nativeInputValueType == 'bsvec':
        input_fp.write('    protected int individualInputValsCount;\n')
        kernelfp.write('    protected int individualInputValsCount;\n')
        if isMapper:
            kernelfp.write('    protected int currentInputVectorLength = -1;\n')
            input_fp.write('    public TreeMap<Integer, LinkedList<IndValWrapper>> sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')
        input_fp.write('    public int nVectorsToBuffer;\n')
    elif nativeInputValueType == 'ivec':
        input_fp.write('    protected int individualInputValsCount;\n')
        kernelfp.write('    protected int individualInputValsCount;\n')
        if isMapper:
            kernelfp.write('    protected int currentInputVectorLength = -1;\n')
            input_fp.write('    public TreeMap<Integer, LinkedList<IndValWrapper>> sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')
        input_fp.write('    public int nVectorsToBuffer;\n')

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec':
        output_fp.write('    protected int[] memAuxIntIncr;\n')
        output_fp.write('    protected int[] memAuxDoubleIncr;\n')
        kernelfp.write('    protected int[] memAuxIntIncr;\n')
        kernelfp.write('    protected int[] memAuxDoubleIncr;\n')
        kernelfp.write('    protected int outputAuxIntLength;\n')
        kernelfp.write('    protected int outputAuxDoubleLength;\n')
    elif nativeOutputValueType == 'ivec':
        output_fp.write('    protected int[] memAuxIncr;\n')
        kernelfp.write('    protected int[] memAuxIncr;\n')
        kernelfp.write('    protected int outputAuxIntLength;\n')
    elif nativeOutputValueType == 'fsvec':
        output_fp.write('    protected int[] memAuxIntIncr;\n')
        output_fp.write('    protected int[] memAuxFloatIncr;\n')
        kernelfp.write('    protected int[] memAuxIntIncr;\n')
        kernelfp.write('    protected int[] memAuxFloatIncr;\n')
        kernelfp.write('    protected int outputAuxIntLength;\n')
        kernelfp.write('    protected int outputAuxFloatLength;\n')

    kernelfp.write('\n')
    input_fp.write('\n')
    output_fp.write('\n')

    writeln(visitor(nativeInputKeyType).getKeyValDecl('inputKey', isMapper, True, True), 1, kernelfp)
    writeln(visitor(nativeInputValueType).getKeyValDecl('inputVal', isMapper, True, True), 1, kernelfp)
    writeln(visitor(nativeOutputKeyType).getKeyValDecl('outputKey', isMapper, False, True), 1, kernelfp)
    writeln(visitor(nativeOutputValueType).getKeyValDecl('outputVal', isMapper, False, True), 1, kernelfp)
    # kernelfp.write('    final private '+hadoopOutputKeyType+'Writable keyObj = new '+hadoopOutputKeyType+'Writable();\n')
    # kernelfp.write('    final private '+hadoopOutputValueType+'Writable valObj = new '+hadoopOutputValueType+'Writable();\n')
    kernelfp.write('\n')
    generateKernelDecl(isMapper, nativeInputKeyType, nativeInputValueType, kernelfp)

    writePostKernelSetupMethod(kernelfp, isMapper, nativeOutputKeyType, nativeOutputValueType)
    writePreKernelSetupMethod(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)
    writeInitMethod(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    kernelfp.write('    public Class<? extends HadoopCLInputBuffer> getInputBufferClass() { return '+inputBufferClassName(isMapper, inputKeyType, inputValueType)+'.class; }\n')
    kernelfp.write('    public Class<? extends HadoopCLOutputBuffer> getOutputBufferClass() { return '+outputBufferClassName(isMapper, outputKeyType, outputValueType)+'.class; }\n')

    if isVariableLength(nativeOutputValueType):
        kernelfp.write('    protected int[] allocInt(int len) {\n')
        kernelfp.write('        return new int[len];\n')
        kernelfp.write('    }\n')
        kernelfp.write('    protected double[] allocDouble(int len) {\n')
        kernelfp.write('        return new double[len];\n')
        kernelfp.write('    }\n')
        kernelfp.write('    protected float[] allocFloat(int len) {\n')
        kernelfp.write('        return new float[len];\n')
        kernelfp.write('    }\n')
        kernelfp.write('\n')
    kernelfp.write('\n')

    if isVariableLength(nativeInputValueType):
        if isMapper:
            kernelfp.write('    protected int inputVectorLength(int vid) {\n')
            kernelfp.write('       return this.currentInputVectorLength;\n')
            kernelfp.write('    }\n')
            kernelfp.write('\n')
        else:
            kernelfp.write('    protected int inputVectorLength(int vid) {\n')
            kernelfp.write('       return 0;\n')
            kernelfp.write('    }\n')

    writeOriginalInputInitMethod(input_fp, nativeInputKeyType, nativeInputValueType)

    generateFill(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)
    generatePrepareForRead(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    # if not isMapper:
    #     input_fp.write('    @Override\n')
    #     input_fp.write('    public void bufferInputValue(Object obj) {\n')
    #     input_fp.write('        '+hadoopInputValueType+'Writable actual = ('+hadoopInputValueType+'Writable)obj;\n')
    #     writeln(visitor(nativeInputValueType).getBufferInputValue(), 2, input_fp)
    #     input_fp.write('    }\n')
    #     input_fp.write('\n')
    #     input_fp.write('    @Override\n')
    #     input_fp.write('    public void useBufferedValues() {\n')
    #     writeln(visitor(nativeInputValueType).getUseBufferedValues(), 2, input_fp)
    #     input_fp.write('        this.nVals += this.tempBuffer1.size();\n')
    #     input_fp.write('    }\n')

    writeAddValueMethod(input_fp, hadoopInputValueType, nativeInputValueType)

    writeAddKeyMethod(input_fp, hadoopInputKeyType, nativeInputKeyType)

    writeIsFullMethod(input_fp, isMapper, nativeInputKeyType, nativeInputValueType, hadoopInputValueType)
    writeResetMethod(input_fp, isMapper, nativeInputValueType)

    # writeTransferBufferedValues(input_fp, isMapper)

    writeToHadoopMethod(output_fp, isMapper, hadoopOutputKeyType, hadoopOutputValueType, nativeOutputKeyType, nativeOutputValueType)

    writeInitBeforeKernelMethod(output_fp, isMapper, nativeOutputKeyType, nativeOutputValueType)

    # writeResetForAnotherAttempt(input_fp, isMapper, nativeInputKeyType, nativeInputValueType)

    writeSpace(input_fp, isMapper, nativeInputKeyType, nativeInputValueType, True)
    writeSpace(output_fp, isMapper, nativeOutputKeyType, nativeOutputValueType, False)

    kernelfp.write('    @Override\n')
    kernelfp.write('    public boolean equalInputOutputTypes() {\n')
    if nativeInputKeyType == nativeOutputKeyType and nativeInputValueType == nativeOutputValueType:
        kernelfp.write('        return true;\n')
    else:
        kernelfp.write('        return false;\n')
    kernelfp.write('    }\n')

    # generateCloneIncompleteMethod(bufferfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    kernelfp.write('\n')
    kernelfp.write('    private final '+hadoopOutputKeyType+'Writable keyObj = new '+hadoopOutputKeyType+'Writable();\n')
    kernelfp.write('    private final '+hadoopOutputValueType+'Writable valObj = new '+hadoopOutputValueType+'Writable();\n')
    generateWriteMethod(kernelfp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType)
    if isVariableLength(nativeOutputValueType):
        generateWriteWithOffsetMethod(kernelfp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType)

    generateKernelCall(isMapper, nativeInputKeyType, nativeInputValueType, kernelfp)

    kernelfp.write('    @Override\n')
    kernelfp.write('    public IHadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context) throws InterruptedException, IOException {\n')
    kernelfp.write('        Context ctx = (Context)context;\n')
    kernelfp.write('        if (this.clContext.doHighLevelProfiling()) {\n')
    kernelfp.write('            this.javaProfile = new HadoopCLAccumulatedProfile();\n')
    kernelfp.write('        } else {\n')
    kernelfp.write('            this.javaProfile = new HadoopCLEmptyAccumulatedProfile();\n')
    kernelfp.write('        }\n')
    kernelfp.write('        this.javaProfile.startOverall();\n')
    if not isMapper:
        writeln(visitor(nativeInputValueType).getBufferedInit(), 2, kernelfp)

    kernelfp.write('        while(ctx.nextKeyValue()) {\n')
    kernelfp.write('            this.javaProfile.startRead();\n')
    if isMapper:
        kernelfp.write('            '+hadoopInputKeyType +'Writable key = ('+hadoopInputKeyType+'Writable)ctx.getCurrentKey();\n')
        kernelfp.write('            '+hadoopInputValueType +'Writable val = ('+hadoopInputValueType+'Writable)ctx.getCurrentValue();\n')
        if isVariableLength(nativeInputValueType):
            kernelfp.write('            this.currentInputVectorLength = val.size();\n')
        kernelfp.write('            this.javaProfile.stopRead();\n')
        kernelfp.write('            this.javaProfile.startKernel();\n')
        kernelfp.write('            map('+tostr_without_last_ln(visitor(nativeInputKeyType).getMapArguments('key'), 0)+', '+
            tostr(visitor(nativeInputValueType).getMapArguments('val'), 0)+');\n')
        kernelfp.write('            this.javaProfile.stopKernel();\n')
    else:
        kernelfp.write('            '+hadoopInputKeyType +'Writable key = ('+hadoopInputKeyType+'Writable)ctx.getCurrentKey();\n')
        kernelfp.write('            Iterable<'+hadoopInputValueType+'Writable> values = (Iterable<'+hadoopInputValueType+'Writable>)ctx.getValues();\n')
        writeln(visitor(nativeInputValueType).getResetHelper(), 3, kernelfp)

        kernelfp.write('            for('+hadoopInputValueType+'Writable v : values) {\n')
        writeln(visitor(nativeInputValueType).getAddValHelper(), 4, kernelfp)

        kernelfp.write('            }\n')
        kernelfp.write('            this.javaProfile.stopRead();\n')
        kernelfp.write('            this.javaProfile.startKernel();\n')
        kernelfp.write('            reduce('+tostr_without_last_ln(visitor(nativeInputKeyType).getMapArguments('key'), 0)+', ')
        writeln(visitor(nativeInputValueType).getJavaProcessReducerCall(), 3, kernelfp)
        kernelfp.write('            this.javaProfile.stopKernel();\n')

    kernelfp.write('            OpenCLDriver.inputsRead++;\n')
    kernelfp.write('        }\n')
    kernelfp.write('        this.javaProfile.stopOverall();\n')
    kernelfp.write('        return this.javaProfile;\n')
    kernelfp.write('    }\n')

    input_fp.write('}\n\n')
    output_fp.write('}\n\n')
    kernelfp.write('}\n\n')

    input_fp.close()
    output_fp.close()
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
