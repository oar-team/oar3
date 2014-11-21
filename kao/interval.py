
def test_and_sub_prefix_itvs(prefix_itvs, itvs):
    #need of sub_intervals
    flag = True
    lx = len(prefix_itvs)
    ly = len(itvs)
    i = 0
    residue_itvs=[]

    if (lx > ly):
        return (False,residue_itvs)

    if (lx == 0):
        return (True,itvs)
    
    while (flag) and (i<lx) and (i<ly):
        x = prefix_itvs[i]
        y = itvs[i]
        i += 1
        if not (x[0]==y[0]) and (x[1]==y[1]):
            flag = False
            i -= 1
                        
    residue_itvs = [x for x in islice(itvs, i, None)]

    return flag, residue_itvs

#test_and_sub_prefix_itvs( [(1,4),(6,9)], [(6,9),(10,17),(20,30)] )
#(false, [{b = 10; e = 17}; {b = 20; e = 30}])

#test_and_sub_prefix_itvs( [(1,4),(6,9)], [(1,4),(6,9),(10,17),(20,30)] )
#(false, [{b = 10; e = 17}; {b = 20; e = 30}])

def equal_itvs(itvs1, itvs2):
    lx = len(itvs1)
    ly = len(itvs2)
    i = 0
    if (lx != ly):
        return False
    if (lx == 0):
        return True
    while (i<lx): 
        x = itvs1[i]
        y = itvs2[i]
        if not ((x[0]==y[0]) and (x[1]==y[1])):
            return False
        i += 1
    return True

def extract_n_scattered_block_itv(itvs1, itvs_ref, n): 
    #itv_l_a lst_itvs_reference n
    #need of test_and_sub_prefix_itvs:
    lr = len(itvs_ref)
    i = 0
    itvs = []
    
    while (n>0) and (i<lr):
        x = itvs_ref[i]
        y = intersec(itvs1, x)
        if equal_itvs(x, y):
            itvs.extend(x)
            n -= 1
        i += 1
    
    if (n==0):
        itvs.sort()
        return itvs
    else:
        return []

#y = [ [(1, 4), (6,9)],  [(10,17)], [(20,30)] ]
#extract_n_scattered_block_itv([(1,30)], y, 3)
#[(1, 4), (6, 9), (10, 17), (20, 30)]

#extract_n_scattered_block_itv ([(1, 12), (15,32)], y, 2)

#y = [ [(1, 4), (10,17)],  [(6,9), (19,22)], [(25,30)] ]


def keep_no_empty_scat_bks(itvs, itvss_ref):
    ''' keep_no_empty_scat_bks : 
    keep no empty scattered blocks where their intersection with itvs is not empty '''
    lr = len(itvss_ref)
    i = 0 
    r_itvss = []

    while(i<lr):
        x = itvss_ref[i]
        if (intersec(x, itvs) != []):
            r_itvss.append(x)
        i += 1
    return r_itvss

def sub_intervals(itvs1,itvs2):
    lx = len(itvs1)
    ly = len(itvs2)
    i = 0
    k = 0   
    itvs = []

    while (i<lx) and (lx>0):
        x = itvs1[i]        
        if (k == ly):
            itvs.append(x)
            i += 1
        else:
            y = itvs2[k]
            #y before x w/ no overlap 
            if (y[1] < x[0]):
                k += 1
            else:   
                # x before y w/ no overlap
                if (y[0] > x[1]):
                    itvs.append(x)
                    i += 1
                else:    
                    if (y[0] > x[0]):
                        if (y[1] < x[1]):
                            # x overlap totally y
                            itvs.append( (x[0],y[0]-1) ) 
                            itvs1[i] = (y[1]+1,x[1])
                            k += 1
                        else:
                            #x overlap partially
                            itvs.append( (x[0],y[0]-1) )
                            i += 1
                    else:
                            if (y[1] < x[1]):
                                #x overlap partially
                                itvs1[i] = (y[1]+1,x[1])
                                k += 1
                            else:
                                # y overlap totally x
                                i += 1

    return itvs


def intersec(itvs1,itvs2):
    lx = len(itvs1)
    ly = len(itvs2)
    i = 0
    k = 0
    itvs = []
    
    while (i<lx) and (lx>0) and (ly>0):
        x = itvs1[i]
        if (k == ly):
            break
        else:
            y = itvs2[k]

        # y before x w/ no overlap 
        if (y[1] < x[0]):
            k += 1
        else:

            # x before y w/ no overlap    
            if (y[0] > x[1]):
                i += 1
            else:    

                if (y[0] >= x[0]):
                    if (y[1] <= x[1]):
                        itvs.append(y)
                        k += 1
                    else:
                        itvs.append( (y[0], x[1]) )
                        i += 1
                else:
                        if (y[1] <= x[1]):
                            itvs.append( (x[0], y[1]) )
                            k += 1
                        else:
                            itvs.append(x)
                            i += 1
                            
    return itvs
