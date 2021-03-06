import sys

primitives = [ 'int', 'float', 'double', 'long' ]
nonprimitives = [ 'pair', 'ipair', 'ppair' ]
variablelength = [ 'bsvec', 'svec', 'ivec', 'fsvec', 'psvec' ]
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
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
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
    def getCloneIncompleteReducerKey(self):
        raise NotImplementedError()
    def getFillParameter(self, basename, isInput, isMapper):
        raise NotImplementedError()
    def getMapArguments(self, varName):
        raise NotImplementedError()
    def getBufferedDecl(self):
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
    def getIteratorComparison(self):
        raise NotImplementedError()
    def getPartitionOfCurrent(self):
        raise NotImplementedError()
    def getKeyFillForIterator(self):
        raise NotImplementedError()
    def getKeyLengthForIterator(self):
        raise NotImplementedError()
    def getValueFillForIterator(self):
        raise NotImplementedError()
    def getValueLengthForIterator(self):
        raise NotImplementedError()
    def getPartitioner(self):
        raise NotImplementedError()
    def getKeyFor(self):
        raise NotImplementedError()
    def getValueFor(self):
        raise NotImplementedError()
    def getSerializeKey(self):
        raise NotImplementedError()
    def getSerializeValue(self):
        raise NotImplementedError()
    def getPreliminaryKeyFullCheck(self, isMapper):
        raise NotImplementedError()
    def getPreliminaryValueFullCheck(self, isMapper):
        raise NotImplementedError()
    def getReadKeyFromStream(self):
        raise NotImplementedError()
    def getReadValueFromStream(self):
        raise NotImplementedError()
    def getStridedReadValueFromStream(self):
        raise NotImplementedError()
    def bulkAddKey(self, isMapper):
        raise NotImplementedError()
    def bulkAddValue(self, isMapper):
        raise NotImplementedError()
    def getSameAsLastKeyMethod(self):
        raise NotImplementedError()
    def getTransferKeyMethod(self):
        raise NotImplementedError()
    def getTransferValueMethod(self):
        raise NotImplementedError()
    def getCompareKeys(self):
        raise NotImplementedError()
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        raise NotImplementedError()

#################################################################################
########################## Visitor for Primitive type ###########################
#################################################################################
class PrimitiveVisitor(NativeTypeVisitor):
    def __init__(self, typ):
        self.typ = typ

    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        return [ 'public '+('final' if isFinal else '')+' '+self.typ+'[] '+basename+'s;' ]
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
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeys[newBuffer.nKeys] = this.inputKeys[i];' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'s' ]
    def getMapArguments(self, varName):
        return [ varName+'.get()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizable'+self.typ.capitalize()+'Array bufferedVals = null;' ]
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
    def getIteratorComparison(self):
        return [ 'final '+self.typ+' aKey = aBuf.outputKeys[a.index];',
                 'final '+self.typ+' bKey = bBuf.outputKeys[b.index];',
                 'if (aKey < bKey) return -1;',
                 'else if (aKey > bKey) return 1;',
                 'else return 0;' ]
    def getPartitionOfCurrent(self):
        if self.typ == 'int':
            return [ 'return (buf.outputKeys[offset] & Integer.MAX_VALUE) % partitions;' ]
        elif self.typ == 'float':
            return [ 'return (Float.floatToIntBits(buf.outputKeys[offset]) & Integer.MAX_VALUE) % partitions;' ]
        elif self.typ == 'double':
            return [ 'return (((int)Double.doubleToLongBits(buf.outputKeys[offset])) & Integer.MAX_VALUE) % partitions;' ]
        elif self.typ == 'long':
            return [ 'return (((int)buf.outputKeys[offset]) & Integer.MAX_VALUE) % partitions;' ]
    def getKeyFillForIterator(self):
        if self.typ == 'int':
            return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 4);',
                     'this.keyBytes.position(0);',
                     'this.keyBytes.putInt(buffer.outputKeys[index.index]);' ]
        elif self.typ == 'float':
            return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 4);',
                     'this.keyBytes.position(0);',
                     'this.keyBytes.putFloat(buffer.outputKeys[index.index]);' ]
        elif self.typ == 'double':
            return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 8);',
                     'this.keyBytes.position(0);',
                     'this.keyBytes.putDouble(buffer.outputKeys[index.index]);' ]
        elif self.typ == 'long':
            return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 8);',
                     'this.keyBytes.position(0);',
                     'this.keyBytes.putLong(buffer.outputKeys[index.index]);' ]
    def getKeyLengthForIterator(self):
        if self.typ == 'int':
            return [ 'return 4;' ]
        elif self.typ == 'float':
            return [ 'return 4;' ]
        elif self.typ == 'double':
            return [ 'return 8;' ]
        elif self.typ == 'long':
            return [ 'return 8;' ]
    def getValueFillForIterator(self):
        if self.typ == 'int':
            return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 4);',
                     'this.valueBytes.position(0);',
                     'this.valueBytes.putInt(buffer.outputVals[index.index]);' ]
        elif self.typ == 'float':
            return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 4);',
                     'this.valueBytes.position(0);',
                     'this.valueBytes.putFloat(buffer.outputVals[index.index]);' ]
        elif self.typ == 'double':
            return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 8);',
                     'this.valueBytes.position(0);',
                     'this.valueBytes.putDouble(buffer.outputVals[index.index]);' ]
        elif self.typ == 'long':
            return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 8);',
                     'this.valueBytes.position(0);',
                     'this.valueBytes.putLong(buffer.outputVals[index.index]);' ]
    def getValueLengthForIterator(self):
        if self.typ == 'int':
            return [ 'return 4;' ]
        elif self.typ == 'float':
            return [ 'return 4;' ]
        elif self.typ == 'double':
            return [ 'return 8;' ]
        elif self.typ == 'long':
            return [ 'return 8;' ]
    def getPartitioner(self):
        if self.typ == 'int':
            return [ 'return (this.outputKeys[index] & Integer.MAX_VALUE) % numReduceTasks;' ]
        elif self.typ == 'float':
            return [ 'return (Float.floatToIntBits(this.outputKeys[index]) & Integer.MAX_VALUE) % numReduceTasks;' ]
        elif self.typ == 'double':
            return [ 'return (((int)Double.doubleToLongBits(this.outputKeys[index])) & Integer.MAX_VALUE) % numReduceTasks;' ]
        elif self.typ == 'long':
            return [ 'return (((int)this.outputKeys[index]) & Integer.MAX_VALUE) % numReduceTasks;' ]
        return [ ]
    def getKeyFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputKeys[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new '+self.typ.capitalize()+'Writable(this.outputKeys[index]);',
                 '}' ]
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputVals[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new '+self.typ.capitalize()+'Writable(this.outputVals[index]);',
                 '}' ]
    def getSerializeKey(self):
        if self.typ == 'int':
            return [ 'out.writeInt(this.outputKeys[index]);' ]
        elif self.typ == 'float':
            return [ 'out.writeFloat(this.outputKeys[index]);' ]
        elif self.typ == 'double':
            return [ 'out.writeDouble(this.outputKeys[index]);' ]
        elif self.typ == 'long':
            return [ 'out.writeLong(this.outputKeys[index]);' ]
        return [ ]
    def getSerializeValue(self):
        if self.typ == 'int':
            return [ 'out.writeInt(this.outputVals[index]);' ]
        elif self.typ == 'float':
            return [ 'out.writeFloat(this.outputVals[index]);' ]
        elif self.typ == 'double':
            return [ 'out.writeDouble(this.outputVals[index]);' ]
        elif self.typ == 'long':
            return [ 'out.writeLong(this.outputVals[index]);' ]
        return [ ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.capacity' ]
        else:
            return [ 'this.nKeys < this.inputKeys.length' ]
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'true' ]
        else:
            return [ 'this.nVals < this.inputVals.length' ]
    def getReadKeyFromStream(self):
        return [ 'final '+self.typ+' tmpKey = stream.read'+self.typ.capitalize()+'();' ]
    def getReadValueFromStream(self):
        return [ 'final '+self.typ+' tmpVal = stream.read'+self.typ.capitalize()+'();' ]
    def getStridedReadValueFromStream(self):
        raise NotImplementedError()
    def bulkAddKey(self, isMapper):
        if isMapper:
            return [ 'this.inputKeys[this.nPairs] = tmpKey;' ]
        else:
            return [ 'if (this.currentKey == null || this.currentKey.get() != tmpKey) {',
                     '    this.keyIndex[this.nKeys] = this.nVals;',
                     '    this.inputKeys[this.nKeys] = tmpKey;',
                     '    this.nKeys++;',
                     '    if (this.currentKey == null) {',
                     '        this.currentKey = new '+self.typ.capitalize()+'Writable(tmpKey);',
                     '    } else {',
                     '        this.currentKey.set(tmpKey);',
                     '    }',
                     '}' ]
    def bulkAddValue(self, isMapper):
        if isMapper:
            return [ 'this.inputVals[this.nPairs] = tmpVal;',
                     'this.nPairs++;' ]
        else:
            return [ 'this.inputVals[this.nVals] = tmpVal;',
                     'this.nVals++;' ]
    def getSameAsLastKeyMethod(self):
        return [ 'return obj != null && (('+self.typ.capitalize()+'Writable)obj).get() == this.inputKeys[this.nKeys-1];' ]
    def getTransferKeyMethod(self):
        return [ 'this.inputKeys[0] = other.inputKeys[other.lastNKeys - 1];' ]
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputVals, this.inputVals, other.nVals, this.nVals);' ]
    def getCompareKeys(self):
        if self.typ == 'int':
            return [ 'return this.readInt() - other.readInt();' ]
        elif self.typ == 'float':
            return [ 'float thisVal = this.readFloat();',
                     'float otherVal = other.readFloat();',
                     'return (thisVal < otherVal ? -1 : (thisVal > otherVal ? 1 : 0));' ]
        elif self.typ == 'double':
            return [ 'double thisVal = this.readDouble();',
                     'double otherVal = other.readDouble();',
                     'return (thisVal < otherVal ? -1 : (thisVal > otherVal ? 1 : 0));' ]
        elif self.typ == 'long':
            return [ 'long thisVal = this.readLong();',
                     'long otherVal = other.readLong();',
                     'return (thisVal < otherVal ? -1 : (thisVal > otherVal ? 1 : 0));' ]
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        if self.typ == 'int':
            return [ 'Integer.toString('+varName+'s['+index+'])' ]
        else:
            return [ self.typ.capitalize()+'.toString('+varName+'s['+index+'])' ]

#################################################################################
########################## Visitor for Pair type ################################
#################################################################################
class PairVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        return [ 'public '+('final' if isFinal else '')+' double[] '+basename+'s1;',
                 'public '+('final' if isFinal else '')+' double[] '+basename+'s2;' ]
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
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'s1, '+basename+'s2' ]
    def getMapArguments(self, varName):
        return [ varName+'.getVal1(), '+varName+'.getVal2()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizableDoubleArray bufferedVal1 = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal2 = null;' ]
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
    def getIteratorComparison(self):
        return [ 'double thisVal1 = aBuf.outputKeys1[a.index];',
                 'double thatVal1 = bBuf.outputKeys1[b.index];',
                 'if(thisVal1 < thatVal1) {',
                 '    return -1;',
                 '} else if (thisVal1 > thatVal1) {',
                 '    return 1;',
                 '} else {',
                 '    double thisVal2 = aBuf.outputKeys1[a.index];',
                 '    double thatVal2 = bBuf.outputKeys2[b.index];',
                 '    if(thisVal2 < thatVal2) {',
                 '        return -1;',
                 '    } else if(thisVal2 > thatVal2) {',
                 '        return 1;',
                 '    } else {',
                 '        return 0;',
                 '    }',
                 '}' ]
    def getPartitionOfCurrent(self):
        return [ 'return (((int)buf.outputKeys1[offset] + (int)buf.outputKeys2[offset]) & Integer.MAX_VALUE) % partitions;' ]
    def getKeyFillForIterator(self):
        return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 16);',
                 'this.keyBytes.position(0);',
                 'this.keyBytes.putDouble(buffer.outputKeys1[index.index]);',
                 'this.keyBytes.putDouble(buffer.outputKeys2[index.index]);' ]
    def getKeyLengthForIterator(self):
        return [ 'return 16;' ]
    def getValueFillForIterator(self):
        return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 16);',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putDouble(buffer.outputVals1[index.index]);',
                 'this.valueBytes.putDouble(buffer.outputVals2[index.index]);' ]
    def getValueLengthForIterator(self):
        return [ 'return 16;' ]
    def getPartitioner(self):
        return [ 'return (((int)this.outputKeys1[index] + (int)this.outputKeys2[index]) & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputKeys1[index], this.outputKeys2[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new PairWritable(this.outputKeys1[index], this.outputKeys2[index]);',
                 '}' ]
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputVals1[index], this.outputVals2[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new PairWritable(this.outputVals1[index], this.outputVals2[index]);',
                 '}' ]
    def getSerializeKey(self):
        return [ 'out.writeDouble(this.outputKeys1[index]);', 'out.writeDouble(this.outputKeys2[index]);' ]
    def getSerializeValue(self):
        return [ 'out.writeDouble(this.outputVals1[index]);',
                 'out.writeDouble(this.outputVals2[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.capacity' ]
        else:
            return [ 'this.nKeys < this.inputKeys1.length' ]
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'true' ]
        else:
            return [ 'this.nVals < this.inputVals1.length' ]
    def getReadKeyFromStream(self):
        return [ 'final double tmpKey1 = stream.readDouble();',
                 'final double tmpKey2 = stream.readDouble();' ]
    def getReadValueFromStream(self):
        return [ 'final double tmpVal1 = stream.readDouble();',
                 'final double tmpVal2 = stream.readDouble();' ]
    def getStridedReadValueFromStream(self):
        raise NotImplementedError()
    def bulkAddKey(self, isMapper):
        if isMapper:
            return [ 'this.inputKeys1[this.nPairs] = tmpKey1;',
                     'this.inputKeys2[this.nPairs] = tmpKey2;' ]
        else:
            return [ 'if (this.currentKey == null || this.currentKey.getVal1() != tmpKey1 || this.currentKey.getVal2() != tmpKey2) {',
                     '    this.keyIndex[this.nKeys] = this.nVals;',
                     '    this.inputKeys1[this.nKeys] = tmpKey1;',
                     '    this.inputKeys2[this.nKeys] = tmpKey2;',
                     '    this.nKeys++;',
                     '    if (this.currentKey == null) {',
                     '        this.currentKey = new PairWritable(tmpKey1, tmpKey2);',
                     '    } else {',
                     '        this.currentKey.set(tmpKey1, tmpKey2);',
                     '    }',
                     '}' ]
    def bulkAddValue(self, isMapper):
        if isMapper:
            return [ 'this.inputVals1[this.nPairs] = tmpVal1;',
                     'this.inputVals2[this.nPairs] = tmpVal2;',
                     'this.nPairs++;' ]
        else:
            return [ 'this.inputVals1[this.nVals] = tmpVal1;',
                     'this.inputVals2[this.nVals] = tmpVal2;',
                     'this.nVals++;' ]
    def getSameAsLastKeyMethod(self):
        return [ 'PairWritable writable = (PairWritable)obj;',
                 'return obj != null && writable.getVal1() == this.inputKeys1[this.nKeys-1] && writable.getVal2() == this.inputKeys2[this.nKeys-1];' ]
    def getTransferKeyMethod(self):
        return [ 'this.inputKeys1[0] = other.inputKeys1[other.lastNKeys - 1];',
                 'this.inputKeys2[0] = other.inputKeys2[other.lastNKeys - 1];' ]
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputVals1, this.inputVals1, other.nVals, this.nVals);',
                 'safeTransfer(other.inputVals2, this.inputVals2, other.nVals, this.nVals);' ]
    def getCompareKeys(self):
        return [ 'final double thisVal1 = this.readDouble();',
                 'final double otherVal1 = other.readDouble();',
                 'if (thisVal1 < otherVal1) {',
                 '    return -1;',
                 '} else if (thisVal1 > otherVal1) {',
                 '    return 1;',
                 '} else {',
                 '    final double thisVal2 = this.readDouble();',
                 '    final double otherVal2 = other.readDouble();',
                 '    if (thisVal2 < otherVal2) {',
                 '        return -1;',
                 '    } else if (thisVal2 > otherVal2) {',
                 '        return 1;',
                 '    } else {',
                 '        return 0;',
                 '    }',
                 '}' ]
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        return [ '"{ "+'+varName+'s1['+index+']+", "+'+varName+'s2['+index+']+" }"' ]

#################################################################################
########################## Visitor for PPair type ###############################
#################################################################################
class PPairVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        return [ 'public '+('final' if isFinal else '')+' int[] '+basename+'s1;',
                 'public '+('final' if isFinal else '')+' double[] '+basename+'s2;' ]
    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        if forceNull:
            return [ basename+'s1 = null;',
                     basename+'s2 = null;' ]
        else:
            return [ basename+'s1 = new int['+size+'];',
                     basename+'s2 = new double['+size+'];' ]
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
        return [ 'int '+basename+'1, double '+basename+'2' ]
    def getWriteMethodBody(self, basename, isKey):
        return [ basename+'Obj.set('+basename+'1, '+basename+'2);' ]
    def getIterArg(self):
        return [ 'HadoopCLPPairValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        return [ 'input'+basename+'s1[3], input'+basename+'s2[3]' ]
    def getKernelCallIter(self):
        return [ 'null' ]
    def getSetupParameter(self, basename, isInput, isKey):
        return [ 'int[] set'+basename.capitalize()+'s1, ',
                 'double[] set'+basename.capitalize()+'s2' ]
    def getSetLengths(self):
        return [ 'this.outputLength = this.getArrayLength("outputVals1");' ]
    def getAddValueMethodMapper(self):
        return [ 'this.inputVals1[this.nPairs] = actual.getId();',
                 'this.inputVals2[this.nPairs] = actual.getVal();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputVals1[this.nVals] = actual.getId();',
                 'this.inputVals2[this.nVals++] = actual.getVal();' ]
    def getAddKeyMethod(self, index):
        return [ 'this.inputKeys1['+index+'] = actual.getId();',
                 'this.inputKeys2['+index+'] = actual.getVal();' ]
    def getLimitSetter(self):
        return [ 'int limit = this.outputKeys1.length < this.memIncr[0] ? this.outputKeys1.length : this.memIncr[0];' ]
    def getCloneIncompleteMapperKey(self):
        return [ 'newBuffer.inputKeys1[newBuffer.nPairs] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nPairs] = this.inputKeys2[i];' ]
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'s1, '+basename+'s2' ]
    def getMapArguments(self, varName):
        return [ varName+'.getId(), '+varName+'.getVal()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizableIntArray bufferedVal1 = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal2 = null;' ]
    def getBufferedInit(self):
        return [ 'this.bufferedVal1 = new HadoopCLResizableIntArray();',
                 'this.bufferedVal2 = new HadoopCLResizableDoubleArray();' ]
    def getResetHelper(self):
        return [ 'this.bufferedVal1.reset();',
                 'this.bufferedVal2.reset();' ]
    def getAddValHelper(self):
        return [ 'this.bufferedVal1.add(v.getId());',
                 'this.bufferedVal2.add(v.getVal());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLPPairValueIterator(
                       (int[])this.bufferedVal1.getArray(),
                       (double[])this.bufferedVal2.getArray(),
                       this.bufferedVal1.size()));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        directionStr = 'input' if isInput else 'output'
        valStr = 'Keys' if isKey else 'Vals'
        base = directionStr + valStr
        if isKey:
            return [ '('+base+'1.length * 4) +',
                     '('+base+'2.length * 8) +' ]
        else:
            return [ '('+base+'1.length * 4) +',
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
    def getIteratorComparison(self):
        return [ 'int thisId = aBuf.outputKeys1[a.index];',
                 'int thatId = bBuf.outputKeys1[b.index];',
                 'return thisId - thatId;' ]
    def getPartitionOfCurrent(self):
        return [ 'return ((buf.outputKeys1[offset]) & Integer.MAX_VALUE) % partitions;' ]
    def getKeyFillForIterator(self):
        return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 12);',
                 'this.keyBytes.position(0);',
                 'this.keyBytes.putInt(buffer.outputKeys1[index.index]);',
                 'this.keyBytes.putDouble(buffer.outputKeys2[index.index]);' ]
    def getKeyLengthForIterator(self):
        return [ 'return 12;' ]
    def getValueFillForIterator(self):
        return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 12);',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putInt(buffer.outputVals1[index.index]);',
                 'this.valueBytes.putDouble(buffer.outputVals2[index.index]);' ]
    def getValueLengthForIterator(self):
        return [ 'return 16;' ]
    def getPartitioner(self):
        return [ 'return ((this.outputKeys1[index]) & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputKeys1[index], this.outputKeys2[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new PPairWritable(this.outputKeys1[index], this.outputKeys2[index]);',
                 '}' ]
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputVals1[index], this.outputVals2[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new PPairWritable(this.outputVals1[index], this.outputVals2[index]);',
                 '}' ]
    def getSerializeKey(self):
        return [ 'out.writeInt(this.outputKeys1[index]);', 'out.writeDouble(this.outputKeys2[index]);' ]
    def getSerializeValue(self):
        return [ 'out.writeInt(this.outputVals1[index]);',
                 'out.writeDouble(this.outputVals2[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.capacity' ]
        else:
            return [ 'this.nKeys < this.inputKeys1.length' ]
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'true' ]
        else:
            return [ 'this.nVals < this.inputVals1.length' ]
    def getReadKeyFromStream(self):
        return [ 'final int tmpKey1 = stream.readInt();',
                 'final double tmpKey2 = stream.readDouble();' ]
    def getReadValueFromStream(self):
        return [ 'final int tmpVal1 = stream.readInt();',
                 'final double tmpVal2 = stream.readDouble();' ]
    def getStridedReadValueFromStream(self):
        raise NotImplementedError()
    def bulkAddKey(self, isMapper):
        if isMapper:
            return [ 'this.inputKeys1[this.nPairs] = tmpKey1;',
                     'this.inputKeys2[this.nPairs] = tmpKey2;' ]
        else:
            return [ 'if (this.currentKey == null || this.currentKey.getId() != tmpKey1) {',
                     '    this.keyIndex[this.nKeys] = this.nVals;',
                     '    this.inputKeys1[this.nKeys] = tmpKey1;',
                     '    this.inputKeys2[this.nKeys] = tmpKey2;',
                     '    this.nKeys++;',
                     '    if (this.currentKey == null) {',
                     '        this.currentKey = new PPairWritable(tmpKey1, tmpKey2);',
                     '    } else {',
                     '        this.currentKey.set(tmpKey1, tmpKey2);',
                     '    }',
                     '}' ]
    def bulkAddValue(self, isMapper):
        if isMapper:
            return [ 'this.inputVals1[this.nPairs] = tmpVal1;',
                     'this.inputVals2[this.nPairs] = tmpVal2;',
                     'this.nPairs++;' ]
        else:
            return [ 'this.inputVals1[this.nVals] = tmpVal1;',
                     'this.inputVals2[this.nVals] = tmpVal2;',
                     'this.nVals++;' ]
    def getSameAsLastKeyMethod(self):
        return [ 'PPairWritable writable = (PPairWritable)obj;',
                 'return obj != null && writable.getId() == this.inputKeys1[this.nKeys-1];' ]
    def getTransferKeyMethod(self):
        return [ 'this.inputKeys1[0] = other.inputKeys1[other.lastNKeys - 1];',
                 'this.inputKeys2[0] = other.inputKeys2[other.lastNKeys - 1];' ]
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputVals1, this.inputVals1, other.nVals, this.nVals);',
                 'safeTransfer(other.inputVals2, this.inputVals2, other.nVals, this.nVals);' ]
    def getCompareKeys(self):
        return [ 'final int thisId = this.readInt();',
                 'final int otherId = other.readInt();',
                 'return thisId - otherId;' ]
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        return [ '"{ "+'+varName+'s1['+index+']+", "+'+varName+'s2['+index+']+" }"' ]


#################################################################################
########################## Visitor for Ipair type ###############################
#################################################################################
class IpairVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        return [ 'public '+('final' if isFinal else '')+' int[] '+basename+'Ids;',
                 'public '+('final' if isFinal else '')+' double[] '+basename+'s1;',
                 'public '+('final' if isFinal else '')+' double[] '+basename+'s2;' ]
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
    def getCloneIncompleteReducerKey(self):
        return [ 'newBuffer.inputKeyIds[newBuffer.nKeys] = this.inputKeyIds[i];',
                 'newBuffer.inputKeys1[newBuffer.nKeys] = this.inputKeys1[i];',
                 'newBuffer.inputKeys2[newBuffer.nKeys] = this.inputKeys2[i];' ]
    def getFillParameter(self, basename, isInput, isMapper):
        return [ basename+'Ids, '+basename+'s1, '+basename+'s2' ]
    def getMapArguments(self, varName):
        return [ varName+'.getIVal(), '+varName+'.getVal1(), '+varName+'.getVal2()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizableIntArray bufferedValId = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal1 = null;',
                 'protected HadoopCLResizableDoubleArray bufferedVal2 = null;' ]
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
    def getIteratorComparison(self):
        return [ 'int thisIval = aBuf.outputKeyIds[a.index];',
                 'double thisVal1 = aBuf.outputKeys1[a.index];',
                 'double thisVal2 = aBuf.outputKeys2[a.index];',
                 'int thatIval = bBuf.outputKeyIds[b.index];',
                 'double thatVal1 = bBuf.outputKeys1[b.index];',
                 'double thatVal2 = bBuf.outputKeys2[b.index];',
                 'if(thisIval < thatIval) {',
                 '    return -1;',
                 '} else if(thisIval > thatIval) {',
                 '    return 1;',
                 '} else {',
                 '    if(thisVal1 < thatVal1) {',
                 '        return -1;',
                 '    } else if (thisVal1 > thatVal1) {',
                 '        return 1;',
                 '    } else {',
                 '        if(thisVal2 < thatVal2) {',
                 '            return -1;',
                 '        } else if(thisVal2 > thatVal2) {',
                 '            return 1;',
                 '        } else {',
                 '            return 0;',
                 '        }',
                 '    }',
                 '}' ]
    def getPartitionOfCurrent(self):
        return [ 'return (buf.outputValIds[offset] & Integer.MAX_VALUE) % partitions;' ]
    def getKeyFillForIterator(self):
        return [ 'this.keyBytes = resizeByteBuffer(this.keyBytes, 20);',
                 'this.keyBytes.position(0);',
                 'this.keyBytes.putInt(   buffer.outputKeyIds[index.index]);',
                 'this.keyBytes.putDouble(buffer.outputKeys1[index.index]);',
                 'this.keyBytes.putDouble(buffer.outputKeys2[index.index]);' ]
    def getKeyLengthForIterator(self):
        return [ 'return 20;' ]
    def getValueFillForIterator(self):
        return [ 'this.valueBytes = resizeByteBuffer(this.valueBytes, 20);',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putInt(   buffer.outputValIds[index.index]);',
                 'this.valueBytes.putDouble(buffer.outputVals1[index.index]);',
                 'this.valueBytes.putDouble(buffer.outputVals2[index.index]);' ]
    def getValueLengthForIterator(self):
        return [ 'return 20;' ]
    def getPartitioner(self):
        return [ 'return (this.outputKeyIds[index] & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputKeyIds[index], this.outputKeys1[index], this.outputKeys2[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new UniquePairWritable(this.outputKeyIds[index], this.outputKeys1[index], this.outputKeys2[index]);',
                 '}' ]
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputValIds[index], this.outputVals1[index], this.outputVals2[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out = new UniquePairWritable(this.outputValIds[index], this.outputVals1[index], this.outputVals2[index]);',
                 '}' ]
    def getSerializeKey(self):
        return [ 'out.writeInt(this.outputKeyIds[index]);', 'out.writeDouble(this.outputKeys1[index]);', 'out.writeDouble(this.outputKeys2[index]);' ]
    def getSerializeValue(self):
        return [ 'out.writeInt(this.outputValIds[index]);',
                 'out.writeDouble(this.outputVals1[index]);',
                 'out.writeDouble(this.outputVals2[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.capacity' ]
        else:
            return [ 'this.nKeys < this.inputKeyIds.length' ]
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'true' ]
        else:
            return [ 'this.nVals < this.inputValIds.length' ]
    def getReadKeyFromStream(self):
        return [ 'final int tmpKeyId = stream.readInt();',
                 'final double tmpKey1 = stream.readDouble();',
                 'final double tmpKey2 = stream.readDouble();' ]
    def getReadValueFromStream(self):
        return [ 'final int tmpValId = stream.readInt();',
                 'final double tmpVal1 = stream.readDouble();',
                 'final double tmpVal2 = stream.readDouble();' ]
    def getStridedReadValueFromStream(self):
        raise NotImplementedError()
    def bulkAddKey(self, isMapper):
        if isMapper:
            return [ 'this.inputKeyIds[this.nPairs] = tmpKeyId;',
                     'this.inputKeys1[this.nPairs] = tmpKey1;',
                     'this.inputKeys2[this.nPairs] = tmpKey2;' ]
        else:
            return [ 'if (this.currentKey == null || this.currentKey.getIVal() != tmpKeyId || this.currentKey.getVal1() != tmpKey1 || this.currentKey.getVal2() != tmpKey2) {',
                     '    this.keyIndex[this.nKeys] = this.nVals;',
                     '    this.inputKeyIds[this.nKeys] = tmpKeyId;',
                     '    this.inputKeys1[this.nKeys] = tmpKey1;',
                     '    this.inputKeys2[this.nKeys] = tmpKey2;',
                     '    this.nKeys++;',
                     '    if (this.currentKey == null) {',
                     '        this.currentKey = new UniquePairWritable(tmpKeyId, tmpKey1, tmpKey2);',
                     '    } else {',
                     '        this.currentKey.set(tmpKeyId, tmpKey1, tmpKey2);',
                     '    }',
                     '}' ]
    def bulkAddValue(self, isMapper):
        if isMapper:
            return [ 'this.inputValIds[this.nPairs] = tmpValId;',
                     'this.inputVals1[this.nPairs] = tmpVal1;',
                     'this.inputVals2[this.nPairs] = tmpVal2;',
                     'this.nPairs++;' ]
        else:
            return [ 'this.inputValIds[this.nVals] = tmpValId;',
                     'this.inputVals1[this.nVals] = tmpVal1;',
                     'this.inputVals2[this.nVals] = tmpVal2;',
                     'this.nVals++;' ]
    def getSameAsLastKeyMethod(self):
        return [ 'UniquePairWritable writable = (UniquePairWritable)obj;',
                 'return obj != null && writable.getVal1() == this.inputKeys1[this.nKeys-1] && writable.getVal2() == this.inputKeys2[this.nKeys-1] && writable.getIVal() == this.inputKeyIds[this.nKeys-1];' ]
    def getTransferKeyMethod(self):
        return [ 'this.inputKeyIds[0] = other.inputKeyIds[other.lastNKeys - 1];',
                 'this.inputKeys1[0] = other.inputKeys1[other.lastNKeys - 1];',
                 'this.inputKeys2[0] = other.inputKeys2[other.lastNKeys - 1];' ]
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputValIds, this.inputValIds, other.nVals, this.nVals);',
                 'safeTransfer(other.inputVals1,  this.inputVals1,  other.nVals, this.nVals);',
                 'safeTransfer(other.inputVals2,  this.inputVals2,  other.nVals, this.nVals);' ]
    def getCompareKeys(self):
        return [ 'final int thisIval = this.readInt();',
                 'final int otherIval = other.readInt();',
                 'if (thisIval < otherIval) {',
                 '    return -1;',
                 '} else if (thisIval > otherIval) {',
                 '    return 1;',
                 '} else {',
                 '    final double thisVal1 = this.readDouble();',
                 '    final double otherVal1 = other.readDouble();',
                 '    if (thisVal1 < otherVal1) {',
                 '        return -1;',
                 '    } else if (thisVal1 > otherVal1) {',
                 '        return 1;',
                 '    } else {',
                 '        final double thisVal2 = this.readDouble();',
                 '        final double otherVal2 = other.readDouble();',
                 '        if (thisVal2 < otherVal2) {',
                 '            return -1;',
                 '        } else if (thisVal2 > otherVal2) {',
                 '            return 1;',
                 '        } else {',
                 '            return 0;',
                 '        }',
                 '    }',
                 '}' ]
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        return [ '"{ "+'+varName+'Ids['+index+']+", "+'+varName+'s1['+index+']+", "+'+varName+'s2['+index+']+" }"' ]

#################################################################################
########################## Visitor for Svec type ################################
#################################################################################
class SvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        buf = [ ]
        if isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'LookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Vals;')
        else:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'DoubleLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Vals;')

        if not isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] outputValLengthBuffer;')
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
                if isMapper:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+')];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                else:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'DoubleLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new double[this.clContext.getPreallocDoubleLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
        if not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize()];')
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
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize());')
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
                 '    System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
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
                 'System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
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
    def getIteratorComparison(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getPartitionOfCurrent(self):
        raise NotImplementedError()
    def getKeyFillForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getKeyLengthForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFillForIterator(self):
        return [ 'final int length = buffer.outputValLengthBuffer[index.index];',
                 'this.valueBytes = resizeByteBuffer(this.valueBytes, 4 + (4 * length) + (8 * length));',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putInt(length);',
                 'this.valueBytes.asIntBuffer().put(buffer.outputValIndices, buffer.outputValIntLookAsideBuffer[index.index], length);',
                 'this.valueBytes.position(4 + (4 * length));',
                 'this.valueBytes.asDoubleBuffer().put(buffer.outputValVals, buffer.outputValDoubleLookAsideBuffer[index.index], length);' ]
    def getValueLengthForIterator(self):
        return [ 'final int length = buffers[index.buffer].outputValLengthBuffer[index.index];',
                 'return 4 + (4 * length) + (8 * length);' ]
    def getPartitioner(self):
        return [ 'return (this.outputValIndices[this.outputValIntLookAsideBuffer[index]] & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out =  new SparseVectorWritable(this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '}' ]
    def getSerializeKey(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getSerializeValue(self):
        return [ 'out.writeInt(this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpIntArray(out, this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpDoubleArray(out, this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        raise NotImplementedError()
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.inputValLookAsideBuffer.length' ]
        else:
            return [ 'this.nVals < this.inputValLookAsideBuffer.length' ]
    def getReadKeyFromStream(self):
        raise NotImplementedError()
    def getReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'if (this.individualInputValsCount + vectorLength > this.inputValIndices.length) {',
                 '    stream.prev();',
                 '    this.isFull = true;',
                 '    return nread;',
                 '}' ]
    def getStridedReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'final int[] indices = new int[vectorLength];',
                 'final double[] vals = new double[vectorLength];',
                 'stream.readFully(indices, 0, vectorLength);',
                 'stream.readFully(vals, 0, vectorLength);',
                 'IndValWrapper wrapper = new IndValWrapper(indices, vals, vectorLength);' ]
    def bulkAddKey(self, isMapper):
        raise NotImplementedError()
    def bulkAddValue(self, isMapper):
        if isMapper:
            incrStr = 'this.nPairs'
        else:
            incrStr = 'this.nVals'
        return [ 'this.inputValLookAsideBuffer['+incrStr+'++] = this.individualInputValsCount;',
                 'stream.readFully(this.inputValIndices, this.individualInputValsCount, vectorLength);',
                 'stream.readFully(this.inputValVals, this.individualInputValsCount, vectorLength);',
                 'this.individualInputValsCount += vectorLength;' ]
    def getSameAsLastKeyMethod(self):
        raise NotImplementedError()
    def getTransferKeyMethod(self):
        raise NotImplementedError()
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputValIndices, this.inputValIndices, other.individualInputValsCount, this.individualInputValsCount);',
                 'safeTransfer(other.inputValVals,    this.inputValVals,    other.individualInputValsCount, this.individualInputValsCount);',
                 'for (int i = 0; i < this.nVals; i++) {',
                 '    this.inputValLookAsideBuffer[i] = other.inputValLookAsideBuffer[other.nVals + i] - other.individualInputValsCount;',
                 '}' ]
    def getCompareKeys(self):
        raise NotImplementedError()
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        if isMapper:
            limitStr = 'this.nPairs'
        else:
            limitStr = 'this.nVals'

        if isInput:
            valOffsetStr = indexOffsetStr = (varName+'LookAsideBuffer')
            lengthStr = ('('+index+' == '+limitStr+'-1 ? (individualInputValsCount - '+varName+'LookAsideBuffer['+index+']) : ('+varName+'LookAsideBuffer['+index+'+1] - '+varName+'LookAsideBuffer['+index+']))')
        else:
            indexOffsetStr = (varName+'IntLookAsideBuffer')
            valOffsetStr = (varName+'DoubleLookAsideBuffer')
            lengthStr = (varName+'LengthBuffer['+index+']')

        return [ '"len="+'+lengthStr+'+" val={ "+sparseVecComponentsToString('+varName+'Indices, '+indexOffsetStr+'['+index+'], '+varName+'Vals, '+valOffsetStr+'['+index+'], '+lengthStr+')+"}"' ]

#################################################################################
########################## Visitor for Ivec type ################################
#################################################################################
class IvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        buf = [ ]
        if isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'LookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'s;')
        else:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'LookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'s;')

        if not isInput:
            buf.append('private '+('final' if isFinal else '')+' int[] bufferOutput = null;')
            buf.append('public '+('final' if isFinal else '')+' int[] outputLengthBuffer;')
        return buf

    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        buf = [ ]
        if forceNull:
            buf.append(basename+'LookAsideBuffer = null;')
            buf.append(basename+' = null;')
        else:
            if isInput:
                if isMapper:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+')];\n')
                    buf.append(basename+'s = new int[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                else:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                    buf.append(basename+'s = new int[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
            else:
                buf.append(basename+'LookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'s = new int[this.clContext.getPreallocIntLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
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
            buf.append('this.'+basename+'s = set'+basename.capitalize()+';')

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
        return [ 'input'+basename+'s, input'+basename+'LookAsideBuffer[3] + this.nPairs + this.individualInputValsCount' ]
    def getKernelCallIter(self):
        return [ 'null' ]
        # return [ 'new HadoopCLIvecValueIterator(null)' ]
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
                 '} else {',
                 '    System.arraycopy(actual.vals(), actual.valsOffset(), this.inputVals, this.individualInputValsCount, actual.size());',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.vals(), actual.valsOffset(), this.inputVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
    def getFillParameter(self, basename, isInput, isMapper):
        buf = [ ]
        if isInput:
            buf.append(basename+'LookAsideBuffer, '+basename+'s')
        else:
            buf.append(basename+'IntLookAsideBuffer, '+basename+'DoubleLookAsideBuffer, '+basename)
        return buf
    def getMapArguments(self, varName):
        return [ varName+'.vals(), '+varName+'.size()' ]
    def getBufferedDecl(self):
        return [ ]
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
                     '(inputVals.length * 4);' ]
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
    def getIteratorComparison(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getPartitionOfCurrent(self):
        raise NotImplementedError()
    def getKeyFillForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getKeyLengthForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFillForIterator(self):
        return [ 'final int length = buffer.outputValLengthBuffer[index.index];',
                 'this.valueBytes = resizeByteBuffer(this.valueBytes, 4 + (4 * length));',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putInt(length);',
                 'this.valueBytes.asIntBuffer().put(buffer.outputVals, bufffer.outputValIntLookAsideBuffer[index.index], length);' ]
    def getValueLengthForIterator(self):
        return [ 'final int length = buffers[index.buffer].outputValLengthBuffer[index.index];',
                 'return 4 + (4 * length);' ]
    def getPartitioner(self):
        return [ 'return (this.outputValLengthBuffer[index] & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputVals, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out =  new IntegerVectorWritable(this.outputVals, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '}' ]
    def getSerializeKey(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getSerializeValue(self):
        return [ 'out.writeInt(this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpIntArray(out, this.outputVals, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        raise NotImplementedError()
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.inputValLookAsideBuffer.length' ]
        else:
            return [ 'this.nVals < this.inputValLookAsideBuffer.length' ]
    def getReadKeyFromStream(self):
        raise NotImplementedError()
    def getReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'if (this.individualInputValsCount + vectorLength > this.inputVals.length) {',
                 '    stream.prev();',
                 '    this.isFull = true;',
                 '    return nread;',
                 '}' ]
    def getStridedReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'final int[] indices = new int[vectorLength];',
                 'stream.readFully(indices, 0, vectorLength);',
                 'IndValWrapper wrapper = new IndValWrapper(indices, vectorLength);' ]
    def bulkAddKey(self, isMapper):
        raise NotImplementedError()
    def bulkAddValue(self, isMapper):
        if isMapper:
            incrStr = 'this.nPairs'
        else:
            incrStr = 'this.nVals'
        return [ 'this.inputValLookAsideBuffer['+incrStr+'++] = this.individualInputValsCount;',
                 'stream.readFully(this.inputVals, this.individualInputValsCount, vectorLength);',
                 'this.individualInputValsCount += vectorLength;' ]
    def getSameAsLastKeyMethod(self):
        raise NotImplementedError()
    def getTransferKeyMethod(self):
        raise NotImplementedError()
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputVals, this.inputVals, other.individualInputValsCount, this.individualInputValsCount);',
                 'for (int i = 0; i < this.nVals; i++) {',
                 '    this.inputValLookAsideBuffer[i] = other.inputValLookAsideBuffer[other.nVals + i] - other.individualInputValsCount;',
                 '}' ]
    def getCompareKeys(self):
        raise NotImplementedError()
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        if isMapper:
            limitStr = 'this.nPairs'
        else:
            limitStr = 'this.nVals'

        if isInput:
            indexOffsetStr = (varName+'LookAsideBuffer')
            lengthStr = ('('+index+' == '+limitStr+'-1 ? (individualInputValsCount - '+varName+'LookAsideBuffer['+index+']) : ('+varName+'LookAsideBuffer['+index+'+1] - '+varName+'LookAsideBuffer['+index+']))')
        else:
            indexOffsetStr = (varName+'IntLookAsideBuffer')
            lengthStr = (varName+'LengthBuffer['+index+']')
        return [ '"len="+'+lengthStr+'+" val={ "+sparseVecComponentsToString('+varName+'s, '+indexOffsetStr+'['+index+'], '+lengthStr+')+"}"' ]

#################################################################################
########################## Visitor for Fsvec type ###############################
#################################################################################
class FsvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        buf = [ ]
        if isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'LookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' float[] '+basename+'Vals;')
        else:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'FloatLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' float[] '+basename+'Vals;')

        if not isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] outputValLengthBuffer;')
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
                if isMapper:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+')];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new float[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                else:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new float[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'FloatLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new float[this.clContext.getPreallocFloatLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
        if not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize()];')
            buf.append('memAuxIntIncr = new int[1];')
            buf.append('memAuxFloatIncr = new int[1];')
        return buf

    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        buf = [ ]
        buf.append('this.arrayLengths.put("'+basename+'IntLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'FloatLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'Indices", this.clContext.getPreallocIntLength());')
        buf.append('this.arrayLengths.put("'+basename+'Vals", this.clContext.getPreallocFloatLength());')
        if not isKey:
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize());')
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
                 '} else {',
                 '    System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
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
    def getIteratorComparison(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getPartitionOfCurrent(self):
        raise NotImplementedError()
    def getKeyFillForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getKeyLengthForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFillForIterator(self):
        return [ 'final int length = buffer.outputValLengthBuffer[index.index];',
                 'this.valueBytes = resizeByteBuffer(this.valueBytes, 4 + (4 * length) + (4 * length));',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putInt(length);',
                 'this.valueBytes.asIntBuffer().put(buffer.outputValIndices, buffer.outputValIntLookAsideBuffer[index.index], length);',
                 'this.valueBytes.position(4 + (4 * length));',
                 'this.valueBytes.asFloatBuffer().put(buffer.outputValVals, buffer.outputValFloatLookAsideBuffer[index.index], length);' ]
    def getValueLengthForIterator(self):
        return [ 'final int length = buffers[index.buffer].outputValLengthBuffer[index.index];',
                 'return 4 + (4 * length) + (4 * length);' ]
    def getPartitioner(self):
        return [ 'return (this.outputValIndices[this.outputValIntLookAsideBuffer[index]] & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValFloatLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out =  new FSparseVectorWritable(this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValFloatLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '}' ]
    def getSerializeKey(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getSerializeValue(self):
        return [ 'out.writeInt(this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpIntArray(out, this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpFloatArray(out, this.outputValVals, this.outputValFloatLookAsideBuffer[index], this.outputValLengthBuffer[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        raise NotImplementedError()
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.inputValLookAsideBuffer.length' ]
        else:
            return [ 'this.nVals < this.inputValLookAsideBuffer.length' ]
    def getReadKeyFromStream(self):
        raise NotImplementedError()
    def getReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'if (this.individualInputValsCount + vectorLength > this.inputValIndices.length) {',
                 '    stream.prev();',
                 '    this.isFull = true;',
                 '    return nread;',
                 '}' ]
    def getStridedReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'final int[] indices = new int[vectorLength];',
                 'final float[] vals = new float[vectorLength];',
                 'stream.readFully(indices, 0, vectorLength);',
                 'stream.readFully(vals, 0, vectorLength);',
                 'IndValWrapper wrapper = new IndValWrapper(indices, vals, vectorLength);' ]
    def bulkAddKey(self, isMapper):
        raise NotImplementedError()
    def bulkAddValue(self, isMapper):
        if isMapper:
            incrStr = 'this.nPairs'
        else:
            incrStr = 'this.nVals'
        return [ 'this.inputValLookAsideBuffer['+incrStr+'++] = this.individualInputValsCount;',
                 'stream.readFully(this.inputValIndices, this.individualInputValsCount, vectorLength);',
                 'stream.readFully(this.inputValVals, this.individualInputValsCount, vectorLength);',
                 'this.individualInputValsCount += vectorLength;' ]
    def getSameAsLastKeyMethod(self):
        raise NotImplementedError()
    def getTransferKeyMethod(self):
        raise NotImplementedError()
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputValIndices, this.inputValIndices, other.individualInputValsCount, this.individualInputValsCount);',
                 'safeTransfer(other.inputValVals,    this.inputValVals,    other.individualInputValsCount, this.individualInputValsCount);',
                 'for (int i = 0; i < this.nVals; i++) {',
                 '    this.inputValLookAsideBuffer[i] = other.inputValLookAsideBuffer[other.nVals + i] - other.individualInputValsCount;',
                 '}' ]
    def getCompareKeys(self):
        raise NotImplementedError()
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        if isMapper:
            limitStr = 'this.nPairs'
        else:
            limitStr = 'this.nVals'

        if isInput:
            valOffsetStr = indexOffsetStr = (varName+'LookAsideBuffer')
            lengthStr = ('('+index+' == '+limitStr+'-1 ? (individualInputValsCount - '+varName+'LookAsideBuffer['+index+']) : ('+varName+'LookAsideBuffer['+index+'+1] - '+varName+'LookAsideBuffer['+index+']))')
        else:
            indexOffsetStr = (varName+'IntLookAsideBuffer')
            valOffsetStr = (varName+'FloatLookAsideBuffer')
            lengthStr = (varName+'LengthBuffer['+index+']')

        return [ '"len="+'+lengthStr+'+" val={ "+sparseVecComponentsToString('+varName+'Indices, '+indexOffsetStr+'['+index+'], '+varName+'Vals, '+valOffsetStr+'['+index+'], '+lengthStr+')+"}"' ]

#################################################################################
########################## Visitor for Bsvec type ###############################
#################################################################################
class BsvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        buf = [ ]
        if isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'LookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Vals;')
        else:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'DoubleLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Vals;')

        if not isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] outputValLengthBuffer;')
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
                if isMapper:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+')];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                else:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'DoubleLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new double[this.clContext.getPreallocDoubleLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
        if not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize()];')
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
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize());')
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
                 '    System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals++] = this.individualInputValsCount;',
                 'System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
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
    def getIteratorComparison(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getPartitionOfCurrent(self):
        raise NotImplementedError()
    def getKeyFillForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getKeyLengthForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFillForIterator(self):
        return [ 'final int length = buffer.outputValLengthBuffer[index.index];',
                 'this.valueBytes = resizeByteBuffer(this.valueBytes, 4 + (4 * length) + (8 * length));',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putInt(length);',
                 'this.valueBytes.asIntBuffer().put(buffer.outputValIndices, buffer.outputValIntLookAsideBuffer[index.index], length);',
                 'this.valueBytes.position(4 + (4 * length));',
                 'this.valueBytes.asDoubleBuffer().put(buffer.outputValVals, buffer.outputValDoubleLookAsideBuffer[index.index], length);' ]
    def getValueLengthForIterator(self):
        return [ 'final int length = buffers[index.buffer].outputValLengthBuffer[index.index];',
                 'return 4 + (4 * length) + (8 * length);' ]
    def getPartitioner(self):
        return [ 'return (this.outputValIndices[this.outputValIntLookAsideBuffer[index]] & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out =  new BSparseVectorWritable(this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '}' ]
    def getSerializeKey(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getSerializeValue(self):
        return [ 'out.writeInt(this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpIntArray(out, this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpDoubleArray(out, this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        raise NotImplementedError()
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.inputValLookAsideBuffer.length' ]
        else:
            return [ 'this.nVals < this.inputValLookAsideBuffer.length' ]
    def getReadKeyFromStream(self):
        raise NotImplementedError()
    def getReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'if (this.individualInputValsCount + vectorLength > this.inputValIndices.length) {',
                 '    stream.prev();',
                 '    this.isFull = true;',
                 '    return nread;',
                 '}' ]
    def getStridedReadValueFromStream(self):
        return [ 'final int vectorLength = stream.readInt();',
                 'final int[] indices = new int[vectorLength];',
                 'final double[] vals = new double[vectorLength];',
                 'stream.readFully(indices, 0, vectorLength);',
                 'stream.readFully(vals, 0, vectorLength);',
                 'IndValWrapper wrapper = new IndValWrapper(indices, vals, vectorLength);' ]
    def bulkAddKey(self, isMapper):
        raise NotImplementedError()
    def bulkAddValue(self, isMapper):
        if isMapper:
            incrStr = 'this.nPairs'
        else:
            incrStr = 'this.nVals'
        return [ 'this.inputValLookAsideBuffer['+incrStr+'++] = this.individualInputValsCount;',
                 'stream.readFully(this.inputValIndices, this.individualInputValsCount, vectorLength);',
                 'stream.readFully(this.inputValVals, this.individualInputValsCount, vectorLength);',
                 'this.individualInputValsCount += vectorLength;' ]
    def getSameAsLastKeyMethod(self):
        raise NotImplementedError()
    def getTransferKeyMethod(self):
        raise NotImplementedError()
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputValIndices, this.inputValIndices, other.individualInputValsCount, this.individualInputValsCount);',
                 'safeTransfer(other.inputValVals,    this.inputValVals,    other.individualInputValsCount, this.individualInputValsCount);',
                 'for (int i = 0; i < this.nVals; i++) {',
                 '    this.inputValLookAsideBuffer[i] = other.inputValLookAsideBuffer[other.nVals + i] - other.individualInputValsCount;',
                 '}' ]
    def getCompareKeys(self):
        raise NotImplementedError()
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        if isMapper:
            limitStr = 'this.nPairs'
        else:
            limitStr = 'this.nVals'

        if isInput:
            valOffsetStr = indexOffsetStr = (varName+'LookAsideBuffer')
            lengthStr = ('('+index+' == '+limitStr+'-1 ? (individualInputValsCount - '+varName+'LookAsideBuffer['+index+']) : ('+varName+'LookAsideBuffer['+index+'+1] - '+varName+'LookAsideBuffer['+index+']))')
        else:
            indexOffsetStr = (varName+'IntLookAsideBuffer')
            valOffsetStr = (varName+'DoubleLookAsideBuffer')
            lengthStr = (varName+'LengthBuffer['+index+']')

        return [ '"len="+'+lengthStr+'+" val={ "+sparseVecComponentsToString('+varName+'Indices, '+indexOffsetStr+'['+index+'], '+varName+'Vals, '+valOffsetStr+'['+index+'], '+lengthStr+')+"}"' ]

#################################################################################
########################## Visitor for Psvec type ###############################
#################################################################################
class PsvecVisitor(NativeTypeVisitor):
    def getKeyValDecl(self, basename, isMapper, isInput, isKernel, isFinal):
        buf = [ ]
        if isInput:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'LookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Probs;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Vals;')
        else:
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'IntLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'DoubleLookAsideBuffer;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Probs;')
            buf.append('public '+('final' if isFinal else '')+' int[] '+basename+'Indices;')
            buf.append('public '+('final' if isFinal else '')+' double[] '+basename+'Vals;')
            buf.append('public '+('final' if isFinal else '')+' int[] outputValLengthBuffer;')
        return buf

    def getKeyValInit(self, basename, size, forceNull, isInput, isMapper, isKey):
        buf = [ ]
        if forceNull:
            if isInput:
                buf.append(basename+'LookAsideBuffer = null;')
            else:
                buf.append(basename+'IntLookAsideBuffer = null;')
                buf.append(basename+'DoubleLookAsideBuffer = null;')
            buf.append(basename+'Probs = null;')
            buf.append(basename+'Indices = null;')
            buf.append(basename+'Vals = null;')
        else:
            if isInput:
                if isMapper:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+')];\n')
                    buf.append(basename+'Probs = new double[('+size+')];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValEleMultiplier()];\n')
                else:
                    buf.append(basename+'LookAsideBuffer = new int[('+size+') * this.clContext.getInputValMultiplier()];\n')
                    buf.append(basename+'Probs = new double[('+size+') * this.clContext.getInputValMultiplier()];\n')
                    buf.append(basename+'Indices = new int[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
                    buf.append(basename+'Vals = new double[('+size+') * this.clContext.getInputValMultiplier() * this.clContext.getInputValEleMultiplier()];\n')
            else:
                buf.append(basename+'IntLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'DoubleLookAsideBuffer = new int['+size+'];\n')
                buf.append(basename+'Probs = new double['+size+'];\n')
                buf.append(basename+'Indices = new int[this.clContext.getPreallocIntLength()];\n')
                buf.append(basename+'Vals = new double[this.clContext.getPreallocDoubleLength()];\n')
        if not isKey and isInput:
            buf.append('this.individualInputValsCount = 0;')
        if not isInput and not isKey:
            buf.append('outputValLengthBuffer = new int[this.clContext.getOutputBufferSize()];')
            buf.append('memAuxIntIncr = new int[1];')
            buf.append('memAuxDoubleIncr = new int[1];')
        return buf

    def getArrayLengthInit(self, basename, size, isMapper, isKey):
        buf = [ ]
        buf.append('this.arrayLengths.put("'+basename+'IntLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'DoubleLookAsideBuffer", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'Probs", '+size+');')
        buf.append('this.arrayLengths.put("'+basename+'Indices", this.clContext.getPreallocIntLength());')
        buf.append('this.arrayLengths.put("'+basename+'Vals", this.clContext.getPreallocDoubleLength());')
        if not isKey:
            buf.append('this.arrayLengths.put("outputValLengthBuffer", this.clContext.getOutputBufferSize());')
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
            buf.append('this.'+basename+'Probs = null;')
            buf.append('this.'+basename+'Indices = null;')
            buf.append('this.'+basename+'Vals = null;')
        else:
            buf.append('this.'+basename+'Probs = set'+basename.capitalize()+'Probs;')
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
        return [ 'double '+basename+'Prob, int[] '+basename+'Indices, double[] '+basename+'Vals, int len' ]
    def getWriteWithOffsetSig(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupport key type bsvec')
        return [ 'double '+basename+'Prob, int[] '+basename+'Indices, int indicesOffset, double[] '+basename+'Vals, int valsOffset, int len' ]
    def getWriteMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type bsvec')
        return [ basename+'Obj.set('+basename+'Prob, '+basename+'Indices, '+basename+'Vals, len);' ]
    def getWriteWithOffsetMethodBody(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type bsvec')
        return [ basename+'Obj.set('+basename+'Prob, '+basename+'Indices, indicesOffset, '+basename+'Vals, valsOffset, len);' ]
    def getIterArg(self):
        return [ 'HadoopCLPsvecValueIterator valIter' ]
    def getKernelCall(self, basename, isKey):
        if isKey:
            raise RuntimeError('Unsupported key type bsvec')
        return [ 'input'+basename+'Probs, input'+basename+'Indices, input'+basename+'Vals, input'+basename+'LookAsideBuffer[3] + this.nPairs + this.individualInputValsCount' ]
    def getKernelCallIter(self):
        return [ 'null' ]
    def getSetupParameter(self, basename, isInput, isKey):
        buf = [ ]
        if isInput:
            buf.append('int[] set'+basename.capitalize()+'LookAsideBuffer, ')
        else:
            buf.append('int[] set'+basename.capitalize()+'IntLookAsideBuffer, ')
            buf.append('int[] set'+basename.capitalize()+'DoubleLookAsideBuffer, ')
        buf.append('double[] set'+basename.capitalize()+'Probs, ')
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
                 '    PIndValWrapper wrapper = new PIndValWrapper(actual.prob(), actual.indices(), actual.vals(), actual.size());',
                 '    if (this.sortedVals.containsKey(actual.size())) {',
                 '        this.sortedVals.get(actual.size()).add(wrapper);',
                 '    } else {',
                 '        LinkedList<IndValWrapper> newList = new LinkedList<PIndValWrapper>();',
                 '        newList.add(wrapper);',
                 '        this.sortedVals.put(actual.size(), newList);',
                 '    }',
                 '} else {',
                 '    this.inputValProbs[this.nPairs] = actual.prob();',
                 '    System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 '    System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 '}',
                 'this.individualInputValsCount += actual.size();' ]
    def getAddValueMethodReducer(self):
        return [ 'this.inputValLookAsideBuffer[this.nVals] = this.individualInputValsCount;',
                 'this.inputValProbs[this.nVals++] = actual.prob();',
                 'System.arraycopy(actual.indices(), actual.indicesOffset(), this.inputValIndices, this.individualInputValsCount, actual.size());',
                 'System.arraycopy(actual.vals(), actual.valsOffset(), this.inputValVals, this.individualInputValsCount, actual.size());',
                 'this.individualInputValsCount += actual.size();' ]
    def getFillParameter(self, basename, isInput, isMapper):
        buf = [ ]
        if isInput:
            buf.append(basename+'LookAsideBuffer, '+basename+'Probs, '+basename+'Indices, '+basename+'Vals')
        else:
            buf.append(basename+'IntLookAsideBuffer, '+basename+'DoubleLookAsideBuffer, '+basename+'Probs, '+basename+'Indices, '+basename+'Vals')
        return buf
    def getMapArguments(self, varName):
        return [ varName+'.prob(), '+varName+'.indices(), '+varName+'.vals(), '+varName+'.size()' ]
    def getBufferedDecl(self):
        return [ 'protected HadoopCLResizableDoubleArray bufferedProbs = null;' ]
    def getBufferedInit(self):
        return [ 'this.bufferedProbs = new HadoopCLResizableDoubleArray();' ]
    def getResetHelper(self):
        return [ 'this.bufferedProbs.reset();',
                 'List<int[]> accIndices = new ArrayList<int[]>();',
                 'List<double[]> accVals = new ArrayList<double[]>();' ]
    def getAddValHelper(self):
        return [ 'bufferedProbs.add(v.prob());',
                 'accIndices.add(v.indices());',
                 'accVals.add(v.vals());' ]
    def getJavaProcessReducerCall(self):
        return [ """new HadoopCLPsvecValueIterator(
                       (double[])bufferedProbs.getArray(), accIndices, accVals));""" ]
    def getSpace(self, isMapper, isInput, isKey):
        # Can't be a key, so isKey always == False
        if isInput:
            if isMapper:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValProbs.length * 8) + ',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
            else:
                return [ '(inputValLookAsideBuffer.length * 4) +',
                         '(inputValProbs.length * 8) + ',
                         '(inputValIndices.length * 4) +',
                         '(inputValVals.length * 8);' ]
        else:
            return [ '(outputValIntLookAsideBuffer.length * 4) +',
                     '(outputValDoubleLookAsideBuffer.length * 4) +',
                     '(outputValProbs.length * 8) + ',
                     '(outputValIndices.length * 4) +',
                     '(outputValVals.length * 8) +',
                     '(outputValLengthBuffer.length * 4) +',
                     '(memAuxIntIncr.length * 4) +',
                     '(memAuxDoubleIncr.length * 4);' ]
    def getOutputLength(self, core, count):
        return [ '(this.output'+core+'IntLookAsideBuffer['+count+'-1]+',
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Probs.length+" prob memory, and "+',
                 'this.output'+core+'Indices.length+" int memory, and "+',
                 '(this.output'+core+'DoubleLookAsideBuffer['+count+'-1]+'
                 'this.output'+core+'LengthBuffer['+count+'-1])+"/"+',
                 'this.output'+core+'Vals.length+" double memory"' ]
    def getInputLength(self, core, isMapper):
        return [ 'this.individualInputValsCount+"/"+this.input'+core+'Indices.length+" '+core.lower()+'s elements"' ]
    def getIteratorComparison(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getPartitionOfCurrent(self):
        raise NotImplementedError()
    def getKeyFillForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getKeyLengthForIterator(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFillForIterator(self):
        return [ 'final int length = buffer.outputValLengthBuffer[index.index];',
                 'this.valueBytes = resizeByteBuffer(this.valueBytes, 8 + 4 + (4 * length) + (8 * length));',
                 'this.valueBytes.position(0);',
                 'this.valueBytes.putDouble(buffer.outputValProbs[index.index];',
                 'this.valueBytes.putInt(length);',
                 'this.valueBytes.asIntBuffer().put(buffer.outputValIndices, buffer.outputValIntLookAsideBuffer[index.index], length);',
                 'this.valueBytes.position(4 + (4 * length));',
                 'this.valueBytes.asDoubleBuffer().put(buffer.outputValVals, buffer.outputValDoubleLookAsideBuffer[index.index], length);' ]
    def getValueLengthForIterator(self):
        return [ 'final int length = buffers[index.buffer].outputValLengthBuffer[index.index];',
                 'return 8 + 4 + (4 * length) + (8 * length);' ]
    def getPartitioner(self):
        return [ 'return (this.outputValIndices[this.outputValIntLookAsideBuffer[index]] & Integer.MAX_VALUE) % numReduceTasks;' ]
    def getKeyFor(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getValueFor(self):
        return [ 'if (ref != null) {',
                 '    ref.set(this.outputValProbs[index], this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '    out = ref;',
                 '} else {',
                 '    out =  new PSparseVectorWritable(this.outputValProbs[index], this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 '}' ]
    def getSerializeKey(self):
        raise NotImplementedError('sparse vector types not supported as keys')
    def getSerializeValue(self):
        return [ 'out.writeDouble(this.outputValProbs[index]);',
                 'out.writeInt(this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpIntArray(out, this.outputValIndices, this.outputValIntLookAsideBuffer[index], this.outputValLengthBuffer[index]);',
                 'this.readUtils.dumpDoubleArray(out, this.outputValVals, this.outputValDoubleLookAsideBuffer[index], this.outputValLengthBuffer[index]);' ]
    def getPreliminaryKeyFullCheck(self, isMapper):
        raise NotImplementedError()
    def getPreliminaryValueFullCheck(self, isMapper):
        if isMapper:
            return [ 'this.nPairs < this.inputValLookAsideBuffer.length' ]
        else:
            return [ 'this.nVals < this.inputValLookAsideBuffer.length' ]
    def getReadKeyFromStream(self):
        raise NotImplementedError()
    def getReadValueFromStream(self):
        return [ 'final double prob = stream.readDouble();',
                 'final int vectorLength = stream.readInt();',
                 'if (this.individualInputValsCount + vectorLength > this.inputValIndices.length) {',
                 '    stream.prev();',
                 '    this.isFull = true;',
                 '    return nread;',
                 '}' ]
    def getStridedReadValueFromStream(self):
        return [ 'final double prob = stream.readDouble();',
                 'final int vectorLength = stream.readInt();',
                 'final int[] indices = new int[vectorLength];',
                 'final double[] vals = new double[vectorLength];',
                 'stream.readFully(indices, 0, vectorLength);',
                 'stream.readFully(vals, 0, vectorLength);',
                 'PIndValWrapper wrapper = new PIndValWrapper(prob, indices, vals, vectorLength);' ]
    def bulkAddKey(self, isMapper):
        raise NotImplementedError()
    def bulkAddValue(self, isMapper):
        if isMapper:
            incrStr = 'this.nPairs'
        else:
            incrStr = 'this.nVals'
        return [ 'this.inputValLookAsideBuffer['+incrStr+'] = this.individualInputValsCount;',
                 'this.inputValProbs['+incrStr+'++] = prob;',
                 'stream.readFully(this.inputValIndices, this.individualInputValsCount, vectorLength);',
                 'stream.readFully(this.inputValVals, this.individualInputValsCount, vectorLength);',
                 'this.individualInputValsCount += vectorLength;' ]
    def getSameAsLastKeyMethod(self):
        raise NotImplementedError()
    def getTransferKeyMethod(self):
        raise NotImplementedError()
    def getTransferValueMethod(self):
        return [ 'safeTransfer(other.inputValProbs, this.inputValProbs, other.nVals, this.nVals);',
                 'safeTransfer(other.inputValIndices, this.inputValIndices, other.individualInputValsCount, this.individualInputValsCount);',
                 'safeTransfer(other.inputValVals,    this.inputValVals,    other.individualInputValsCount, this.individualInputValsCount);',
                 'for (int i = 0; i < this.nVals; i++) {',
                 '    this.inputValLookAsideBuffer[i] = other.inputValLookAsideBuffer[other.nVals + i] - other.individualInputValsCount;',
                 '}' ]
    def getCompareKeys(self):
        raise NotImplementedError()
    def getOutputStrMethod(self, varName, index, isInput, isMapper):
        if isMapper:
            limitStr = 'this.nPairs'
        else:
            limitStr = 'this.nVals'

        if isInput:
            valOffsetStr = indexOffsetStr = (varName+'LookAsideBuffer')
            lengthStr = ('('+index+' == '+limitStr+'-1 ? (individualInputValsCount - '+varName+'LookAsideBuffer['+index+']) : ('+varName+'LookAsideBuffer['+index+'+1] - '+varName+'LookAsideBuffer['+index+']))')
        else:
            indexOffsetStr = (varName+'IntLookAsideBuffer')
            valOffsetStr = (varName+'DoubleLookAsideBuffer')
            lengthStr = (varName+'LengthBuffer['+index+']')

        return [ '"len="+'+lengthStr+'+" val={ "+probVecComponentsToString('+varName+'Probs['+index+'], '+varName+'Indices, '+indexOffsetStr+'['+index+'], '+varName+'Vals, '+valOffsetStr+'['+index+'], '+lengthStr+')+"}"' ]


#################################################################################
########################## End of visitors ######################################
#################################################################################

visitorsMap = { 'int': PrimitiveVisitor('int'),
                'float': PrimitiveVisitor('float'),
                'double': PrimitiveVisitor('double'),
                'long': PrimitiveVisitor('long'),
                'pair': PairVisitor(),
                'ppair': PPairVisitor(),
                'ipair': IpairVisitor(),
                'svec': SvecVisitor(),
                'ivec': IvecVisitor(),
                'fsvec': FsvecVisitor(),
                'bsvec': BsvecVisitor(),
                'psvec': PsvecVisitor() }

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
    elif t == 'ppair':
        return 'PPair'
    elif t == 'svec':
        return 'SparseVector'
    elif t == 'ivec':
        return 'IntegerVector'
    elif t == 'fsvec':
        return 'FSparseVector'
    elif t == 'bsvec':
        return 'BSparseVector'
    elif t == 'psvec':
        return 'PSparseVector'
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
    fp.write('import java.io.DataInput;\n')
    fp.write('import java.util.Collections;\n')
    fp.write('import java.util.Deque;\n')
    fp.write('import java.util.Stack;\n')
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
    fp.write('import java.util.TreeSet;\n')
    fp.write('import java.nio.ByteBuffer;\n')
    fp.write('import java.util.Comparator;\n')
    fp.write('import java.io.DataOutputStream;\n')
    fp.write('import org.apache.hadoop.io.ReadArrayUtils;\n')
    if isMapper:
        fp.write('import org.apache.hadoop.mapreduce.Mapper.Context;\n')
    else:
        fp.write('import org.apache.hadoop.mapreduce.Reducer.Context;\n')
    fp.write('\n')

def writeOutputBufferInit(isMapper, fp, nativeOutputKeyType, nativeOutputValueType):
    writeln(visitor(nativeOutputKeyType).getKeyValInit('outputKey',
        'this.clContext.getOutputBufferSize()', False, False,
            isMapper, True), 3, fp)
    writeln(visitor(nativeOutputValueType).getKeyValInit('outputVal',
        'this.clContext.getOutputBufferSize()', False, False,
            isMapper, False), 3, fp)

def writeInputBufferConstructor(fp, nativeInputKeyType, nativeInputValueType, isMapper):
    fp.write('\n')
    fp.write('    public '+inputBufferClassName(isMapper, nativeInputKeyType, nativeInputValueType)+'(HadoopOpenCLContext clContext, Integer id) {\n')
    fp.write('        super(clContext, id);\n')
    fp.write('\n')

    writeln(visitor(nativeInputKeyType).getKeyValInit('inputKey',
        'this.clContext.getInputBufferSize()', False, True, isMapper, True), 2, fp)

    writeln(visitor(nativeInputValueType).getKeyValInit('inputVal',
        'this.clContext.getInputBufferSize()', False, True, isMapper, False), 2, fp)

    fp.write('    }\n')
    fp.write('\n')

def write_with_indent(fp, indent, s):
    fp.write(('    ' * indent)+s)

def writeBulkFillMethod(fp, nativeInputKeyType, nativeInputValueType, isMapper):
    base_indent = 2
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public final void printContents() {\n')
    if isMapper:
        fp.write('        for (int i = 0; i < nPairs; i++) {\n')
        fp.write('            System.err.println("Reading key="+' + \
            tostr_without_last_ln(visitor(nativeInputKeyType).getOutputStrMethod('inputKey', 'i', True, isMapper), 0) + '+" "+' + \
            tostr_without_last_ln(visitor(nativeInputValueType).getOutputStrMethod('inputVal', 'i', True, isMapper), 0)+');\n')
    else:
        fp.write('        for (int i = 0; i < nKeys; i++) {\n')
        fp.write('            final String keyStr = ' + \
            tostr_without_last_ln(visitor(nativeInputKeyType).getOutputStrMethod('inputKey', 'i', True, isMapper), 0)+';\n')
        fp.write('        for (int j = this.keyIndex[i]; j < (i == nKeys-1 ? nVals : this.keyIndex[i+1]); j++) {\n')
        fp.write('            System.err.println("Reading key="+keyStr+" "+' + \
            tostr_without_last_ln(visitor(nativeInputValueType).getOutputStrMethod('inputVal', 'j', True, isMapper), 0)+');\n')
        fp.write('        }\n')
    fp.write('        }\n')
    fp.write('    }\n')
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public final int bulkFill(HadoopCLDataInput stream) throws IOException {\n')
    fp.write('        int nread = 0;\n')

    if isMapper and isVariableLength(nativeInputValueType):
        base_indent = base_indent + 1
        fp.write('        if (this.enableStriding) {\n')
        fp.write('            while (stream.hasMore() &&\n')
        fp.write('                    '+tostr_without_last_ln(visitor(nativeInputKeyType).getPreliminaryKeyFullCheck(isMapper), 0)+') {\n')
        fp.write('                stream.nextKey();\n')
        writeln(visitor(nativeInputKeyType).getReadKeyFromStream(), base_indent + 1, fp)
        fp.write('                stream.nextValue();\n')
        writeln(visitor(nativeInputValueType).getStridedReadValueFromStream(), base_indent + 1, fp)
        fp.write('                if (this.sortedVals.containsKey(vectorLength)) {\n')
        fp.write('                    this.sortedVals.get(vectorLength).add(wrapper);\n')
        fp.write('                } else {\n')
        fp.write('                    LinkedList<IndValWrapper> newList = new LinkedList<IndValWrapper>();\n')
        fp.write('                    newList.add(wrapper);\n')
        fp.write('                    this.sortedVals.put(vectorLength, newList);\n')
        fp.write('                }\n')
        fp.write('                this.individualInputValsCount += vectorLength;\n')
        fp.write('                nread++;\n')
        fp.write('            }\n')
        fp.write('            if (!('+tostr_without_last_ln(visitor(nativeInputKeyType).getPreliminaryKeyFullCheck(isMapper), 0)+')) {\n')
        fp.write('                this.isFull = true;\n')
        fp.write('            }\n')
        fp.write('        } else {\n')

    write_with_indent(fp, base_indent, 'while (stream.hasMore() &&\n')
    write_with_indent(fp, base_indent, '        '+tostr_without_last_ln(visitor(nativeInputKeyType).getPreliminaryKeyFullCheck(isMapper), 0)+' &&\n')
    write_with_indent(fp, base_indent, '        '+tostr_without_last_ln(visitor(nativeInputValueType).getPreliminaryValueFullCheck(isMapper), 0)+') {\n')
    write_with_indent(fp, base_indent, '    stream.nextKey();\n')
    writeln(visitor(nativeInputKeyType).getReadKeyFromStream(), base_indent + 1, fp)
    write_with_indent(fp, base_indent, '    stream.nextValue();\n')
    writeln(visitor(nativeInputValueType).getReadValueFromStream(), base_indent + 1, fp)
    writeln(visitor(nativeInputKeyType).bulkAddKey(isMapper), base_indent + 1, fp)
    writeln(visitor(nativeInputValueType).bulkAddValue(isMapper), base_indent + 1, fp)
    write_with_indent(fp, base_indent, '    nread++;\n')
    write_with_indent(fp, base_indent, '}\n')
    write_with_indent(fp, base_indent, 'if (!('+tostr_without_last_ln(visitor(nativeInputKeyType).getPreliminaryKeyFullCheck(isMapper), 0)+') ||\n')
    write_with_indent(fp, base_indent, '    !('+tostr_without_last_ln(visitor(nativeInputValueType).getPreliminaryValueFullCheck(isMapper), 0)+')) {\n')
    write_with_indent(fp, base_indent, '    this.isFull = true;\n')
    write_with_indent(fp, base_indent, '}\n')
    if isMapper and isVariableLength(nativeInputValueType):
        fp.write('        }\n')
    fp.write('        return nread;\n')
    fp.write('    }\n')

def writeOutputBufferConstructor(fp, isMapper, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    public '+outputBufferClassName(isMapper, nativeOutputKeyType, nativeOutputValueType)+'(HadoopOpenCLContext clContext, Integer id) {\n')
    fp.write('        super(clContext, id);\n')
    fp.write('\n')
    writeOutputBufferInit(isMapper, fp, nativeOutputKeyType, nativeOutputValueType)
    fp.write('    }\n')
    fp.write('\n')

def writePostKernelSetupDeclaration(fp, isMapper, nativeOutputKeyType, nativeOutputValueType):
    fp.write('    public void postKernelSetup(')
    write(visitor(nativeOutputKeyType).getSetupParameter('outputKey', False, True), 0, fp)
    fp.write(', ')
    write(visitor(nativeOutputValueType).getSetupParameter('outputVal', False, False), 0, fp)

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec' or nativeOutputValueType == 'psvec':
        fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxDoubleIncr')
    elif nativeOutputValueType == 'ivec':
        fp.write(', int[] setMemAuxIncr')
    elif nativeOutputValueType == 'fsvec':
        fp.write(', int[] setMemAuxIntIncr, int[] setMemAuxFloatIncr')

    fp.write(', int[] setMemIncr, int[] setMemWillRequireRestart, int[] setNWrites, int[] outputIterMarkers) {\n')


def writePostKernelSetupMethod(fp, isMapper, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    writePostKernelSetupDeclaration(fp, isMapper, nativeOutputKeyType, nativeOutputValueType)

    writeln(visitor(nativeOutputKeyType).getKeyValSetup('outputKey', False, True, False), 2, fp)
    writeln(visitor(nativeOutputValueType).getKeyValSetup('outputVal', False, False, False), 2, fp)

    fp.write('\n')
    fp.write('        this.memIncr = setMemIncr;\n')
    fp.write('        this.nWrites = setNWrites;\n')
    fp.write('        this.memWillRequireRestart = setMemWillRequireRestart;\n')
    fp.write('\n')

    fp.write('        this.outputIterMarkers = outputIterMarkers;\n')

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec' or nativeOutputValueType == 'psvec':
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

    if isMapper:
        fp.write(', int[] setNWrites, int setNPairs')
    else:
        fp.write(', int[] setKeyIndex, int[] setNWrites, int setNKeys, int setNVals')

    if isVariableLength(nativeInputValueType):
        fp.write(', int setIndividualInputValsCount')
    fp.write(') {\n')

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
    fp.write('        this.memWillRequireRestart = null;\n')
    fp.write('\n')

    writeln(visitor(nativeOutputValueType).getSetLengths(), 2, fp)
    fp.write('        this.outputIterMarkers = null;\n')

    if isVariableLength(nativeInputValueType):
        fp.write('        this.individualInputValsCount = setIndividualInputValsCount;\n')
        fp.write('\n')

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec' or nativeOutputValueType == 'psvec':
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

def writeKernelConstructor(fp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    public '+kernelClassName(isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)+'() {\n')
    fp.write('        super(null, null);\n')
    fp.write('        throw new UnsupportedOperationException();\n')
    fp.write('    }\n')
    fp.write('    public '+kernelClassName(isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)+'(HadoopOpenCLContext clContext, Integer id) {\n')
    fp.write('        super(clContext, id);\n')
    fp.write('        this.setStrided(clContext.runningOnGPU());\n')
    fp.write('\n')
    fp.write('        this.arrayLengths.put("outputIterMarkers", this.clContext.getOutputBufferSize());\n')
    fp.write('        this.arrayLengths.put("memIncr", 1);\n')
    fp.write('        this.arrayLengths.put("memWillRequireRestart", 1);\n')
    writeln(visitor(nativeOutputKeyType).getArrayLengthInit('outputKey',
        'this.clContext.getOutputBufferSize()',
            isMapper, True), 2, fp)
    writeln(visitor(nativeOutputValueType).getArrayLengthInit('outputVal',
        'this.clContext.getOutputBufferSize()',
            isMapper, False), 2, fp)
    fp.write('    }\n')
    fp.write('\n')

def writeAddValueMethod(fp, hadoopInputValueType, nativeInputValueType, isMapper):
    fp.write('    @Override\n')
    fp.write('    public final void addTypedValue(Object val) {\n')
    fp.write('        '+hadoopInputValueType+'Writable actual = ('+hadoopInputValueType+'Writable)val;\n')

    if isMapper:
        writeln(visitor(nativeInputValueType).getAddValueMethodMapper(), 2, fp)
    else:
        writeln(visitor(nativeInputValueType).getAddValueMethodReducer(), 2, fp)

    fp.write('    }\n')
    fp.write('\n')

def writeAddKeyMethod(fp, hadoopInputKeyType, nativeInputKeyType, isMapper):
    fp.write('    @Override\n')
    fp.write('    public final void addTypedKey(Object key) {\n')
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

def writeSameAsLastKeyMethod(fp, nativeInputKeyType):
    fp.write('    @Override\n')
    fp.write('    public boolean sameAsLastKey(Object obj) {\n')
    writeln(visitor(nativeInputKeyType).getSameAsLastKeyMethod(), 2, fp)
    fp.write('    }\n')

def writeRemoveLastKeyMethod(fp, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public void removeLastKey() {\n')
    fp.write('        this.lastNKeys = this.nKeys;\n')
    fp.write('        this.lastNVals = this.nVals;\n')
    if isVariableLength(nativeInputValueType):
        fp.write('        this.lastIndividualInputValsCount = this.individualInputValsCount;\n')

    fp.write('        this.nKeys = this.nKeys - 1;\n')
    fp.write('        this.nVals = this.keyIndex[this.nKeys];\n')
    if isVariableLength(nativeInputValueType):
        fp.write('        this.individualInputValsCount = this.inputValLookAsideBuffer[this.keyIndex[this.nKeys]];\n')
    fp.write('    }\n')

def writeTransferLastKeyMethod(fp, nativeInputKeyType, nativeInputValueType):
    bufferName = inputBufferClassName(False, nativeInputKeyType, nativeInputValueType)
    fp.write('    @Override\n')
    fp.write('    public void transferLastKey(HadoopCLInputReducerBuffer otherBuffer) {\n')
    fp.write('        final '+bufferName+' other = ('+bufferName+')otherBuffer;\n')
    fp.write('        this.nKeys = 1;\n')
    fp.write('        this.nVals = other.lastNVals - other.nVals;\n')
    if isVariableLength(nativeInputValueType):
        fp.write('        this.individualInputValsCount = other.lastIndividualInputValsCount - other.individualInputValsCount;\n')
    writeln(visitor(nativeInputKeyType).getTransferKeyMethod(), 2, fp)
    writeln(visitor(nativeInputValueType).getTransferValueMethod(), 2, fp)
    fp.write('    }\n')

def writeResetMethod(fp, isMapper, nativeInputValueType):
    fp.write('    @Override\n')
    fp.write('    public void reset() {\n')
    if isMapper:
        fp.write('        this.nPairs = 0;\n')
        if nativeInputValueType == 'psvec':
            fp.write('        this.individualInputValsCount = 0;\n')
            fp.write('        this.sortedVals = new TreeMap<Integer, LinkedList<PIndValWrapper>>();\n')
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
        fp.write('        this.lastNKeys = -1;\n')
        fp.write('        this.lastNVals = -1;\n')
        if isVariableLength(nativeInputValueType):
            fp.write('        this.individualInputValsCount = 0;\n')
            fp.write('        this.lastIndividualInputValsCount = -1;\n')
        fp.write('        this.currentKey = null;\n')

    fp.write('        this.isFull = false;\n')
    fp.write('    }\n')
    fp.write('\n')

def writeIsFullMethod(fp, isMapper, nativeInputKeyType, nativeInputValueType, hadoopInputValueType):
    fp.write('    @Override\n')
    fp.write('    public final boolean isFull(TaskInputOutputContext context) throws IOException, InterruptedException {\n')
    fp.write('        if (this.doingBulkRead) {\n')
    fp.write('            return this.isFull;\n')
    fp.write('        } else {\n')
    if isMapper:
        if isVariableLength(nativeInputValueType):
            javaType = None
            arrayName = None
            if nativeInputValueType == 'psvec':
                javaType = 'PSparseVectorWritable'
                arrayName = 'inputValIndices'
            elif nativeInputValueType == 'svec':
                javaType = 'SparseVectorWritable'
                arrayName = 'inputValIndices'
            elif nativeInputValueType == 'bsvec':
                javaType = 'BSparseVectorWritable'
                arrayName = 'inputValIndices'
            elif nativeInputValueType == 'ivec':
                javaType = 'IntegerVectorWritable'
                arrayName = 'inputVals'
            elif nativeInputValueType == 'fsvec':
                javaType = 'FSparseVectorWritable'
                arrayName = 'inputValIndices'
            fp.write('            if (this.enableStriding) {\n')
            fp.write('                return this.nPairs == this.capacity;\n')
            fp.write('            } else {\n')
            fp.write('                return this.nPairs == this.capacity || this.individualInputValsCount +\n')
            fp.write('                    (('+javaType+')((Context)context).getCurrentValue()).size() > this.'+arrayName+'.length;\n')
            fp.write('            }\n')
        else:
            fp.write('            return this.nPairs == this.capacity;\n')
    else:
        fp.write('            Context reduceContext = (Context)context;\n')
        if nativeInputKeyType == 'pair' or nativeInputKeyType == 'ipair' or nativeInputKeyType == 'ppair':
          keysName = 'inputKeys1'
        else:
          keysName = 'inputKeys'

        fp.write('            return (this.nKeys == this.'+keysName+'.length ||\n')
        if isVariableLength(nativeInputValueType):
          fp.write('                this.nVals == this.inputValLookAsideBuffer.length ||\n')
          fp.write('                this.individualInputValsCount + (('+hadoopInputValueType+'Writable)reduceContext.getCurrentValue()).size() > this.inputValIndices.length);\n')
        elif nativeInputValueType == 'pair' or nativeInputValueType == 'ipair':
          fp.write('                this.nVals == this.inputVals1.length);\n')
        else:
          fp.write('                this.nVals == this.inputVals.length);\n')

    fp.write('        }\n')
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
        fp.write(line.replace('DUMMY', 'i + j'))

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
    fp.write('            return -1;\n')

def writeToHadoopMethod(fp, isMapper, hadoopOutputKeyType, hadoopOutputValueType, nativeOutputKeyType, nativeOutputValueType):
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public final int getCount() {\n')
    fp.write('        final int count;\n')
    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec' or nativeOutputValueType == 'psvec':
        fp.write('        if(this.memIncr[0] < 0 || this.outputValIntLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValIntLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
    elif  nativeOutputValueType == 'ivec':
        fp.write('        if(this.memIncr[0] < 0 || this.outputValLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
    elif nativeOutputValueType == 'fsvec':
        fp.write('        if(this.memIncr[0] < 0 || this.outputValIntLookAsideBuffer.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputValIntLookAsideBuffer.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
    else:
        fp.write('        if (this.memIncr[0] < 0 || this.outputIterMarkers.length < this.memIncr[0]) {\n')
        fp.write('            count = this.outputIterMarkers.length;\n')
        fp.write('        } else {\n')
        fp.write('            count = this.memIncr[0];\n')
        fp.write('        }\n')
    fp.write('        return count;\n')
    fp.write('    }\n')
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public final void printContents() {\n')
    fp.write('        if (getCount() == 0) {\n')
    fp.write('            System.err.println("Empty buffer");\n')
    fp.write('            return;\n')
    fp.write('        }\n')
    fp.write('        for (int i = 0; i < getCount(); i++) {\n')
    fp.write('            if (!isValid(i)) continue;\n')
    fp.write('            System.err.println("Writing key="+'+ \
        tostr_without_last_ln(visitor(nativeOutputKeyType).getOutputStrMethod('outputKey', 'i', False, isMapper), 0) + '+" "+' + \
        tostr_without_last_ln(visitor(nativeOutputValueType).getOutputStrMethod('outputVal', 'i', False, isMapper), 0)+');\n')
    fp.write('        }\n')
    fp.write('    }\n')
    fp.write('\n')
    fp.write('    @Override\n')
    fp.write('    public final int putOutputsIntoHadoop(TaskInputOutputContext context, int soFar) throws IOException, InterruptedException {\n')
    fp.write('        this.start = soFar; this.end = getCount();\n')
    fp.write('        return context.writeCollection(this);\n')
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

    if nativeOutputValType == 'svec' or nativeOutputValType == 'bsvec' or nativeOutputValType == 'psvec':
        fp.write(', outputBuffer.memAuxIntIncr, outputBuffer.memAuxDoubleIncr')
    elif nativeOutputValType == 'ivec':
        fp.write(', outputBuffer.memAuxIncr')
    elif nativeOutputValType == 'fsvec':
        fp.write(', outputBuffer.memAuxIntIncr, outputBuffer.memAuxFloatIncr')

    fp.write(', outputBuffer.memIncr, outputBuffer.memWillRequireRestart, outputBuffer.nWrites, outputBuffer.outputIterMarkers);\n')
    fp.write('    }\n')
    fp.write('\n')

def generateFill(fp, isMapper, nativeInputKeyType, nativeInputValType, nativeOutputKeyType, nativeOutputValType):
    inputBufferClass = inputBufferClassName(isMapper, nativeInputKeyType, nativeInputValType)
    fp.write('    @Override\n')
    fp.write('    public void fill(HadoopCLInputBuffer genericInputBuffer) {\n')
    fp.write('        '+inputBufferClass+' inputBuffer = ('+inputBufferClass+')genericInputBuffer;\n')

    if isMapper and isVariableLength(nativeInputValType):
        fp.write('        if (inputBuffer.enableStriding) {\n')
        fp.write('            int index = 0;\n')
        fp.write('            inputBuffer.individualInputValsCount = 0;\n')
        fp.write('            final int maxLength = inputBuffer.sortedVals.lastKey();\n')
        fp.write('            final int nMaxLengthVectors = inputBuffer.sortedVals.get(maxLength).size();\n')
        fp.write('            final int maxBufLength = ((nMaxLengthVectors - 1) + ((maxLength - 1) * inputBuffer.nPairs)) + 1;\n')
        if nativeInputValType == 'svec' or nativeInputValType == 'bsvec' or nativeInputValType == 'psvec':
            fp.write('            if (inputBuffer.inputValIndices.length < maxBufLength) {\n')
            fp.write('                inputBuffer.inputValIndices = new int[maxBufLength];\n')
            fp.write('                inputBuffer.inputValVals = new double[maxBufLength];\n')
            fp.write('            }\n')
        elif nativeInputValType == 'fsvec':
            fp.write('            if (inputBuffer.inputValIndices.length < maxBufLength) {\n')
            fp.write('                inputBuffer.inputValIndices = new int[maxBufLength];\n')
            fp.write('                inputBuffer.inputValVals = new float[maxBufLength];\n')
            fp.write('            }\n')
        else:
            fp.write('            if (inputBuffer.inputVals.length < maxBufLength) {\n')
            fp.write('                inputBuffer.inputVals = new int[maxBufLength];\n')
            fp.write('            }\n')
        fp.write('            Iterator<Integer> lengthIter = inputBuffer.sortedVals.descendingKeySet().iterator();\n')
        fp.write('            while (lengthIter.hasNext()) {\n')
        fp.write('                LinkedList<IndValWrapper> pairs = inputBuffer.sortedVals.get(lengthIter.next());\n')
        fp.write('                Iterator<IndValWrapper> pairsIter = pairs.iterator();\n')
        fp.write('                while (pairsIter.hasNext()) {\n')
        fp.write('                    IndValWrapper curr = pairsIter.next();\n')
        fp.write('                    inputBuffer.inputValLookAsideBuffer[index] = inputBuffer.individualInputValsCount;\n')
        fp.write('                    for (int i = 0; i < curr.length; i++) {\n')
        if nativeInputValType == 'svec' or nativeInputValType == 'fsvec' or nativeInputValType == 'bsvec' or nativeInputValType == 'psvec':
            fp.write('                        inputBuffer.inputValIndices[index + (i * inputBuffer.nPairs)] = curr.indices[i];\n')
            prefix = 'd' if (nativeInputValType == 'svec' or nativeInputValType == 'bsvec' or nativeInputValType == 'psvec') else 'f'
            fp.write('                        inputBuffer.inputValVals[index + (i * inputBuffer.nPairs)] = curr.'+prefix+'vals[i];\n')
        else:
            fp.write('                        inputBuffer.inputVals[index + (i * inputBuffer.nPairs)] = curr.indices[i];\n')
        fp.write('                    }\n')
        fp.write('                    inputBuffer.individualInputValsCount += curr.length;\n')
        fp.write('                    index++;\n')
        fp.write('                } // while (pairsIter)\n')
        fp.write('            } // while (lengthIter)\n')
        fp.write('        } // if (enableStriding)\n')
        fp.write('\n')
    fp.write('        this.preKernelSetup(')

    write(visitor(nativeInputKeyType).getFillParameter('inputBuffer.inputKey', True, isMapper), 0, fp)
    fp.write(', ')
    write(visitor(nativeInputValType).getFillParameter('inputBuffer.inputVal', True, isMapper), 0, fp)

    if isMapper:
        fp.write(', inputBuffer.nWrites, inputBuffer.nPairs')
    else:
        fp.write(', inputBuffer.keyIndex, inputBuffer.nWrites, inputBuffer.nKeys, inputBuffer.nVals')

    if isVariableLength(nativeInputValType):
        fp.write(', inputBuffer.individualInputValsCount')

    fp.write(');\n')
    fp.write('    }\n')
    fp.write('\n')

def writeGetPartitionFor(fp, nativeOutputKeyType):
    fp.write('    @Override\n')
    fp.write('    public final int getPartitionFor(int index, int numReduceTasks) {\n')
    writeln(visitor(nativeOutputKeyType).getPartitioner(), 2, fp)
    fp.write('    }\n')
    fp.write('\n')

def writeKeyValueIteratorDefs(fp, nativeOutputKeyType, nativeOutputValueType, isMapper):
    outputBufferName = outputBufferClassName(isMapper, nativeOutputKeyType, nativeOutputValueType)
    enclosingBufferName = ''.join( [ typeNameForClassName(nativeOutputKeyType),
            typeNameForClassName(nativeOutputValueType), 'HadoopCLOutput',
            ('Mapper' if isMapper else 'Reducer'), 'Buffer' ] )
    fp.write('    public class KeyValueIterator extends HadoopCLKeyValueIterator {\n')
    fp.write('        protected '+outputBufferName+'[] buffers;\n')
    fp.write('        private final '+enclosingBufferName+' buf;\n')
    fp.write('\n')
    fp.write('        public KeyValueIterator(List<OutputBufferSoFar> toWrite, final int numReduceTasks,\n')
    fp.write('                final '+enclosingBufferName+' buf) {\n')
    fp.write('            this.buf = buf;\n')
    fp.write('            this.buffers = new '+outputBufferName+'[toWrite.size()];\n')
    fp.write('            final int[] sofars = new int[this.buffers.length];\n')
    fp.write('            int count = 0;\n')
    fp.write('            for (OutputBufferSoFar tmp : toWrite) {\n')
    fp.write('                sofars[count] = tmp.soFar();\n')
    fp.write('                this.buffers[count++] = ('+outputBufferName+')tmp.buffer();\n')
    fp.write('            }\n')
    fp.write('            this.sortedIndices = new ArrayList<IntegerPair>();\n')
    fp.write('\n')
    fp.write('            for (count = 0; count < this.buffers.length; count++) {\n')
    fp.write('                final HadoopCLOutputBuffer tmpBuf = this.buffers[count];\n')
    fp.write('                final int soFar = sofars[count];\n')
    fp.write('                for (int i = soFar; i < tmpBuf.getCount(); i++) {\n')
    fp.write('                    if (!tmpBuf.itersFinished.contains(tmpBuf.outputIterMarkers[i])) continue;\n')
    fp.write('                    sortedIndices.add(new IntegerPair(count, i));\n')
    fp.write('                }\n')
    fp.write('            }\n')
    fp.write('            Collections.sort(this.sortedIndices, new Comparator<IntegerPair>() {\n')
    fp.write('                @Override\n')
    fp.write('                public int compare(IntegerPair a, IntegerPair b) {\n')
    fp.write('                    final '+outputBufferName+' aBuf = buffers[a.buffer];\n')
    fp.write('                    final '+outputBufferName+' bBuf = buffers[b.buffer];\n')
    fp.write('                    final int aPart = aBuf.getPartitionFor(a.index, numReduceTasks);\n')
    fp.write('                    final int bPart = bBuf.getPartitionFor(b.index, numReduceTasks);\n')
    fp.write('                    if (aPart != bPart) return aPart - bPart;\n')
    writeln(visitor(nativeOutputKeyType).getIteratorComparison(), 5, fp)
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public boolean equals(Object i) {\n')
    fp.write('                    throw new UnsupportedOperationException();\n')
    fp.write('                }\n')
    fp.write('            });\n')
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        @Override\n')
    fp.write('        public int getPartitionOfCurrent(int partitions) {\n')
    fp.write('            final '+outputBufferName+' buf = buffers[current.buffer];\n')
    fp.write('            final int offset = current.index;\n')
    writeln(visitor(nativeOutputKeyType).getPartitionOfCurrent(), 3, fp)
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        public ByteBuffer getKeyFor(IntegerPair index) throws IOException {\n')
    fp.write('            final '+outputBufferName+' buffer = buffers[index.buffer];\n')
    writeln(visitor(nativeOutputKeyType).getKeyFillForIterator(), 3, fp)
    fp.write('            return this.keyBytes;\n')
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        public int getLengthForKey(IntegerPair index) {\n')
    writeln(visitor(nativeOutputKeyType).getKeyLengthForIterator(), 3, fp)
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        @Override\n')
    fp.write('        public final DataInputBuffer getKey() throws IOException {\n')
    fp.write('            final ByteBuffer tmp = getKeyFor(this.current);\n')
    fp.write('            this.key.reset(tmp.array(), 0, getLengthForKey(this.current));\n')
    fp.write('            return this.key;\n')
    fp.write('        }\n')
    fp.write('\n')
    fp.write('       public ByteBuffer getValueFor(IntegerPair index) throws IOException {\n')
    fp.write('            final '+outputBufferName+' buffer = buffers[index.buffer];\n')
    writeln(visitor(nativeOutputValueType).getValueFillForIterator(), 3, fp)
    fp.write('            return this.valueBytes;\n')
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        public int getLengthForValue(IntegerPair index) {\n')
    writeln(visitor(nativeOutputValueType).getValueLengthForIterator(), 3, fp)
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        @Override\n')
    fp.write('        public final DataInputBuffer getValue() throws IOException {\n')
    fp.write('            final ByteBuffer tmp = getValueFor(this.current);\n')
    fp.write('            this.value.reset(tmp.array(), 0, getLengthForValue(this.current));\n')
    fp.write('            return this.value;\n')
    fp.write('        }\n')
    fp.write('\n')
    fp.write('        @Override\n')
    fp.write('        public HadoopCLDataInput getBulkReader() {\n')
    fp.write('            return new HadoopCLBulkMapperReader() {\n')
    fp.write('                private int sortedIndicesIter = 0;\n')
    fp.write('                @Override\n')
    fp.write('                public final boolean hasMore() {\n')
    fp.write('                    return sortedIndicesIter < sortedIndices.size();\n')
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public int compareKeys(HadoopCLDataInput other) throws IOException {\n')
    writeln(visitor(nativeOutputKeyType).getCompareKeys(), 5, fp)
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public final void nextKey() throws IOException {\n')
    fp.write('                    this.current = sortedIndices.get(sortedIndicesIter++);\n')
    fp.write('                    this.currentBuffer = getKeyFor(this.current);\n')
    fp.write('                    this.currentBufferPosition = 0;\n')
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public final void nextValue() throws IOException {\n')
    fp.write('                    this.currentBuffer = getValueFor(this.current);\n')
    fp.write('                    this.currentBufferPosition = 0;\n')
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public final void prev() {\n')
    fp.write('                    this.current = sortedIndices.get(--sortedIndicesIter);\n')
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public final void readFully(int[] b, int off, int len) {\n')
    fp.write('                    this.currentBuffer.position(this.currentBufferPosition);\n')
    fp.write('                    this.currentBufferPosition += (len * 4);\n')
    fp.write('                    this.currentBuffer.asIntBuffer().get(b, off, len);\n')
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public final void readFully(double[] b, int off, int len) {\n')
    fp.write('                    this.currentBuffer.position(this.currentBufferPosition);\n')
    fp.write('                    this.currentBufferPosition += (len * 8);\n')
    fp.write('                    this.currentBuffer.asDoubleBuffer().get(b, off, len);\n')
    fp.write('                }\n')
    fp.write('                @Override\n')
    fp.write('                public final int readInt() {\n')
    fp.write('                    this.currentBuffer.position(this.currentBufferPosition);\n')
    fp.write('                    this.currentBufferPosition += 4;\n')
    fp.write('                    return this.currentBuffer.getInt();\n')
    fp.write('                }\n')
    fp.write('            };\n')
    fp.write('        }\n')
    fp.write('\n')
    fp.write('    }\n')
    fp.write('\n')
    fp.write('    }\n')
    fp.write('\n')


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

    kernelfp = open('mapred/org/apache/hadoop/mapreduce/'+
        kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+'.java', 'w')

    writeHeader(input_fp, isMapper);
    writeHeader(output_fp, isMapper);
    writeHeader(kernelfp, isMapper);

    input_fp.write('public final class '+
            inputBufferClassName(isMapper, inputKeyType, inputValueType)+' extends HadoopCLInput'+
            capitalizedKernelString(isMapper)+'Buffer {\n')
    output_fp.write('public final class '+
            outputBufferClassName(isMapper, outputKeyType, outputValueType)+' extends HadoopCLOutput'+
            capitalizedKernelString(isMapper)+'Buffer implements KVCollection<'+hadoopOutputKeyType+'Writable, '+hadoopOutputValueType+'Writable> {\n')

    kernelfp.write('public abstract class '+
        kernelClassName(isMapper, inputKeyType, inputValueType, outputKeyType, outputValueType)+' extends HadoopCL'+
        capitalizedKernelString(isMapper)+'Kernel {\n')

    writeln(visitor(nativeInputKeyType).getKeyValDecl('inputKey', isMapper, True, False, False), 1, input_fp)
    writeln(visitor(nativeInputValueType).getKeyValDecl('inputVal', isMapper, True, False, False), 1, input_fp)
    writeln(visitor(nativeOutputKeyType).getKeyValDecl('outputKey', isMapper, False, False, False), 1, output_fp)
    writeln(visitor(nativeOutputValueType).getKeyValDecl('outputVal', isMapper, False, False, False), 1, output_fp)

    kernelfp.write('    public int outputLength;\n')

    if not isMapper:
        writeln(visitor(nativeInputValueType).getBufferedDecl(), 1, kernelfp)
        input_fp.write('    private '+hadoopInputKeyType+'Writable currentKey;\n')

    kernelfp.write('\n')
    if nativeInputValueType == 'svec' or nativeInputValueType == 'fsvec' or nativeInputValueType == 'bsvec' or nativeInputValueType == 'psvec':
        input_fp.write('    public int individualInputValsCount;\n')
        input_fp.write('    public int lastIndividualInputValsCount;\n')
        kernelfp.write('    public int individualInputValsCount;\n')
        if isMapper:
            kernelfp.write('    public int currentInputVectorLength = -1;\n')
            input_fp.write('    public TreeMap<Integer, LinkedList<IndValWrapper>> sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')
    elif nativeInputValueType == 'ivec':
        input_fp.write('    public int individualInputValsCount;\n')
        input_fp.write('    public int lastIndividualInputValsCount;\n')
        kernelfp.write('    public int individualInputValsCount;\n')
        if isMapper:
            kernelfp.write('    protected int currentInputVectorLength = -1;\n')
            input_fp.write('    public TreeMap<Integer, LinkedList<IndValWrapper>> sortedVals = new TreeMap<Integer, LinkedList<IndValWrapper>>();\n')

    if nativeOutputValueType == 'svec' or nativeOutputValueType == 'bsvec' or nativeOutputValueType == 'psvec':
        output_fp.write('    public int[] memAuxIntIncr;\n')
        output_fp.write('    public int[] memAuxDoubleIncr;\n')
        kernelfp.write('    public int[] memAuxIntIncr;\n')
        kernelfp.write('    public int[] memAuxDoubleIncr;\n')
        kernelfp.write('    public int outputAuxIntLength;\n')
        kernelfp.write('    public int outputAuxDoubleLength;\n')
    elif nativeOutputValueType == 'ivec':
        output_fp.write('    public final int[] memAuxIncr;\n')
        kernelfp.write('    public int[] memAuxIncr;\n')
        kernelfp.write('    public int outputAuxIntLength;\n')
    elif nativeOutputValueType == 'fsvec':
        output_fp.write('    public int[] memAuxIntIncr;\n')
        output_fp.write('    public int[] memAuxFloatIncr;\n')
        kernelfp.write('    public int[] memAuxIntIncr;\n')
        kernelfp.write('    public int[] memAuxFloatIncr;\n')
        kernelfp.write('    public int outputAuxIntLength;\n')
        kernelfp.write('    public int outputAuxFloatLength;\n')
    output_fp.write('    private int start, end;\n')

    kernelfp.write('\n')
    input_fp.write('\n')
    output_fp.write('\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final int start() { return this.start; }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final int end() { return this.end; }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final boolean isValid(int index) {\n')
    output_fp.write('        return this.itersFinished.contains(this.outputIterMarkers[index]);\n')
    output_fp.write('    }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final Iterator<Integer> iterator() {\n')
    output_fp.write('        return new OutputIterator(this.start, this.end, this.itersFinished, this.outputIterMarkers);\n')
    output_fp.write('    }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final Writable getKeyFor(int index, Writable genericRef) {\n')
    output_fp.write('        final '+hadoopOutputKeyType+'Writable ref = ('+hadoopOutputKeyType+'Writable)genericRef;\n')
    output_fp.write('        final '+hadoopOutputKeyType+'Writable out;\n')
    writeln(visitor(nativeOutputKeyType).getKeyFor(), 2, output_fp);
    output_fp.write('        return out;\n')
    output_fp.write('    }\n')
    output_fp.write('\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final Writable getValueFor(int index, Writable genericRef) {\n')
    output_fp.write('        final '+hadoopOutputValueType+'Writable ref = ('+hadoopOutputValueType+'Writable)genericRef;\n')
    output_fp.write('        final '+hadoopOutputValueType+'Writable out;\n')
    writeln(visitor(nativeOutputValueType).getValueFor(), 2, output_fp);
    output_fp.write('        return out;\n')
    output_fp.write('    }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final void serializeKey(int index, DataOutputStream out) throws IOException {\n')
    writeln(visitor(nativeOutputKeyType).getSerializeKey(), 2, output_fp)
    output_fp.write('    }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public final void serializeValue(int index, DataOutputStream out) throws IOException {\n')
    writeln(visitor(nativeOutputValueType).getSerializeValue(), 2, output_fp)
    output_fp.write('    }\n')
    output_fp.write('\n')


    writeln(visitor(nativeInputKeyType).getKeyValDecl('inputKey', isMapper, True, True, False), 1, kernelfp)
    writeln(visitor(nativeInputValueType).getKeyValDecl('inputVal', isMapper, True, True, False), 1, kernelfp)
    writeln(visitor(nativeOutputKeyType).getKeyValDecl('outputKey', isMapper, False, True, False), 1, kernelfp)
    writeln(visitor(nativeOutputValueType).getKeyValDecl('outputVal', isMapper, False, True, False), 1, kernelfp)
    kernelfp.write('\n')
    generateKernelDecl(isMapper, nativeInputKeyType, nativeInputValueType, kernelfp)

    writePostKernelSetupMethod(kernelfp, isMapper, nativeOutputKeyType, nativeOutputValueType)
    writePreKernelSetupMethod(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)
    writeKernelConstructor(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

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

    writeInputBufferConstructor(input_fp, nativeInputKeyType, nativeInputValueType, isMapper)
    writeBulkFillMethod(input_fp, nativeInputKeyType, nativeInputValueType, isMapper)

    generateFill(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)
    generatePrepareForRead(kernelfp, isMapper, nativeInputKeyType, nativeInputValueType, nativeOutputKeyType, nativeOutputValueType)

    writeAddKeyMethod(input_fp, hadoopInputKeyType, nativeInputKeyType, isMapper)
    writeAddValueMethod(input_fp, hadoopInputValueType, nativeInputValueType, isMapper)

    writeIsFullMethod(input_fp, isMapper, nativeInputKeyType, nativeInputValueType, hadoopInputValueType)
    writeResetMethod(input_fp, isMapper, nativeInputValueType)

    if not isMapper:
        writeSameAsLastKeyMethod(input_fp, nativeInputKeyType)
        writeRemoveLastKeyMethod(input_fp, nativeInputValueType)
        writeTransferLastKeyMethod(input_fp, nativeInputKeyType, nativeInputValueType)

    writeToHadoopMethod(output_fp, isMapper, hadoopOutputKeyType, hadoopOutputValueType, nativeOutputKeyType, nativeOutputValueType)
    output_fp.write('    @Override\n')
    output_fp.write('    public Class<?> getOutputKeyClass() { return '+hadoopOutputKeyType+'Writable.class; }\n')
    output_fp.write('    @Override\n')
    output_fp.write('    public Class<?> getOutputValClass() { return '+hadoopOutputValueType+'Writable.class; }\n')

    writeOutputBufferConstructor(output_fp, isMapper, nativeOutputKeyType, nativeOutputValueType)

    writeSpace(input_fp, isMapper, nativeInputKeyType, nativeInputValueType, True)
    writeSpace(output_fp, isMapper, nativeOutputKeyType, nativeOutputValueType, False)

    kernelfp.write('    @Override\n')
    kernelfp.write('    public boolean equalInputOutputTypes() {\n')
    if nativeInputKeyType == nativeOutputKeyType and nativeInputValueType == nativeOutputValueType:
        kernelfp.write('        return true;\n')
    else:
        kernelfp.write('        return false;\n')
    kernelfp.write('    }\n')

    kernelfp.write('\n')
    kernelfp.write('    private final '+hadoopOutputKeyType+'Writable keyObj = new '+hadoopOutputKeyType+'Writable();\n')
    kernelfp.write('    private final '+hadoopOutputValueType+'Writable valObj = new '+hadoopOutputValueType+'Writable();\n')
    generateWriteMethod(kernelfp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType)
    if isVariableLength(nativeOutputValueType):
        generateWriteWithOffsetMethod(kernelfp, nativeOutputKeyType, nativeOutputValueType, hadoopOutputKeyType, hadoopOutputValueType)

    generateKernelCall(isMapper, nativeInputKeyType, nativeInputValueType, kernelfp)

    kernelfp.write('    @Override\n')
    kernelfp.write('    public IHadoopCLAccumulatedProfile javaProcess(TaskInputOutputContext context, final boolean shouldIncr) throws InterruptedException, IOException {\n')
    kernelfp.write('        Context ctx = (Context)context;\n')
    kernelfp.write('        int lastCount = 0;\n')
    kernelfp.write('        int count = 0;\n')
    kernelfp.write('        long lastTime = System.currentTimeMillis();\n')
    kernelfp.write('        if (this.clContext.doHighLevelProfiling()) {\n')
    kernelfp.write('            this.javaProfile = new HadoopCLAccumulatedProfile();\n')
    kernelfp.write('        } else {\n')
    kernelfp.write('            this.javaProfile = new HadoopCLEmptyAccumulatedProfile();\n')
    kernelfp.write('        }\n')
    kernelfp.write('        this.javaProfile.startOverall();\n')
    if not isMapper:
        writeln(visitor(nativeInputValueType).getBufferedInit(), 2, kernelfp)

    kernelfp.write('        this.javaProfile.startRead();\n')
    kernelfp.write('        while(ctx.nextKeyValue()) {\n')
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
        kernelfp.write('            if (shouldIncr) {\n')
        kernelfp.write('                count++;\n')
        kernelfp.write('                if (count - lastCount > 500) {\n')
        kernelfp.write('                    OpenCLDriver.addProgress(count - lastCount, System.currentTimeMillis() - lastTime);\n')
        kernelfp.write('                    lastTime = System.currentTimeMillis();\n')
        kernelfp.write('                    lastCount = count;\n')
        kernelfp.write('                }\n')
        kernelfp.write('            }\n')
    else:
        kernelfp.write('            '+hadoopInputKeyType +'Writable key = ('+hadoopInputKeyType+'Writable)ctx.getCurrentKey();\n')
        kernelfp.write('            Iterable<'+hadoopInputValueType+'Writable> values = (Iterable<'+hadoopInputValueType+'Writable>)ctx.getValues();\n')
        kernelfp.write('            int countValues = 0;\n')
        writeln(visitor(nativeInputValueType).getResetHelper(), 3, kernelfp)

        kernelfp.write('            for('+hadoopInputValueType+'Writable v : values) {\n')
        writeln(visitor(nativeInputValueType).getAddValHelper(), 4, kernelfp)
        kernelfp.write('                countValues++;\n')
        kernelfp.write('            }\n')
        kernelfp.write('            this.javaProfile.stopRead();\n')
        kernelfp.write('            this.javaProfile.startKernel();\n')
        kernelfp.write('            reduce('+tostr_without_last_ln(visitor(nativeInputKeyType).getMapArguments('key'), 0)+', ')
        writeln(visitor(nativeInputValueType).getJavaProcessReducerCall(), 3, kernelfp)
        kernelfp.write('            this.javaProfile.stopKernel();\n')
        kernelfp.write('            if (shouldIncr) {\n')
        kernelfp.write('                count += countValues;\n')
        kernelfp.write('                if (count - lastCount > 500) {\n')
        kernelfp.write('                    OpenCLDriver.addProgress(count - lastCount, System.currentTimeMillis() - lastTime);\n')
        kernelfp.write('                    lastTime = System.currentTimeMillis();\n')
        kernelfp.write('                    lastCount = count;\n')
        kernelfp.write('                }\n')
        kernelfp.write('            }\n')

    kernelfp.write('            this.javaProfile.startRead();\n')
    kernelfp.write('        }\n')
    kernelfp.write('        this.javaProfile.stopOverall();\n')
    kernelfp.write('        if (shouldIncr) {\n')
    kernelfp.write('            OpenCLDriver.addProgress(count - lastCount, System.currentTimeMillis() - lastTime);\n')
    kernelfp.write('        }\n')
    kernelfp.write('        return this.javaProfile;\n')
    kernelfp.write('    }\n')

    writeGetPartitionFor(output_fp, nativeOutputKeyType)
    output_fp.write('\n')

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
