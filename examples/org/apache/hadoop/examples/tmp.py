#for i in range(63):
#    print 'private int d0_'+str(i)+';'
#for i in range(40):
#    print 'private int d1_'+str(i)+';'

K = [ 63, 40 ]

#print 'long k;'
#for i in range(len(K)):
#    print 'k = index;'
#    print 'x'+str(i)+' = 0;'
#    for j in range(K[i]):
#        if j == 0:
#            print 'q'+str(i)+'_'+str(j)+' = 1.0 / P'+str(i)+';'
#        else:
#            print 'q'+str(i)+'_'+str(j)+' = q'+str(i)+'_'+str(j-1)+' / P'+str(i)+';'
#        print 'd'+str(i)+'_'+str(j)+' = (int)(k % P'+str(i)+');'
#        print 'k = (k - d'+str(i)+'_'+str(j)+') / P'+str(i)+';'
#        print 'x'+str(i)+' += d'+str(i)+'_'+str(j)+' * q'+str(i)+'_'+str(j)+';'
#    print ''

for i in range(len(K)):
    for j in range(K[i]):
        print 'd'+str(i)+'_'+str(j)+'++;'
        print 'x'+str(i)+' += q'+str(i)+'_'+str(j)+';'
        print 'if(d'+str(i)+'_'+str(j)+' < P'+str(i)+') {'
        print '  break;'
        print '}'
        print 'd'+str(i)+'_'+str(j)+' = 0;'
        if j == 0:
            print 'x'+str(i)+' -= 1.0;'
        else:
            print 'x'+str(i)+' -= q'+str(i)+'_'+str(j-1)+';'

