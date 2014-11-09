from interval import *

class Hierarchy:
    #TODO extract hierarchy from ressources table
    def __init__(self,hy):
        self.hy = hy

def find_resource_hierarchies_scattered(itvs, hy, rqts):
    l_hy = len(hy)
    if (l_hy == 1):
        return extract_n_scattered_block_itv(itvs, hy[0], rqts[0])
    else:
        return find_resource_n_h(itvs, hy, rqts, hy[0], 0, l_hy)

def find_resource_n_h(itvs, hy, rqts, top, h, h_bottom):

    #potentiel available blocks
    avail_bks = keep_no_empty_scat_bks(itvs, top) 
    l_avail_bks = len(avail_bks)

    if (l_avail_bks < rqts[h]): 
        #not enough scattered blocks
        return []
    else:
        if (h==h_bottom-2):
            #reach last level hierarchy of requested resource
            #iter on top and find rqts[h-1] block
            itvs_acc = []
            i = 0
            nb_r = 0
            while (i<l_avail_bks) and (nb_r != rqts[h]):  #need
                print avail_bks[i], "*", hy[h+1]
                #TODO test cosf of [] filtering .....
                avail_sub_bks = [ intersec(avail_bks[i],x) for x in hy[h+1] if intersec(avail_bks[i],x) != [] ] 
                print avail_sub_bks
                print "--------------------------------------"
                r = extract_n_scattered_block_itv(itvs, avail_sub_bks, rqts[h+1])
                #r = []
                if (r != []):
                    #win for this top_block
                    itvs_acc.extend(r)
                    nb_r += 1
                i += 1
            if  (nb_r == rqts[h]):
                return itvs_acc
            else:
                return []
                
        else: 
            #intermediate hierarchy level
            #iter on available_bk
            itvs_acc = []
            i = 0 
            nb_r = 0
            while (i < l_avail_bks) and (nb_r != rqts[h]):
                r = find_resource_n_h(itvs, hy, rqts, [avail_bks[i]], h+1, h_bottom)
                if (r != []):
                    #win for this top_block
                    itvs_acc.extend(r)
                    nb_r += 1
                i += 1
            if  (nb_r == rqts[h]):
                return itvs_acc
            else:
                return []

#def G(Y):
#    if one h level:
#        extract
#    else:
#        F(X)
#
#                    
#def F(X):     
#    avail_bks
#    return if not enough bks
#    if bottom-1:
#       take (
#        intersec (avail_kbs[i] h+1)
#        extract )
#    else:
#        take ( find(new X new level) )


#h0 = [[(1, 16)],[(17, 32)]]
#h1 = [[(1,8)],[(9,16)],[(17,24)],[(25,32)]]
#h2 = [[(1,4)], [(5,8)], [(8,12)], [(13,16)], [(17,20)],[(21,24)],[(25,28)],[(29,32)]]

#find_resource_hierarchies_scattered ([(1, 32)], [h0,h1,h2], [2,1,1])

#find_resource_hierarchies_scattered ([(1, 32)], [h0], [2])
#[(1, 16), (17, 32)]
#find_resource_hierarchies_scattered ([(10, 32)], [h0], [1])
#[(17, 32)]
#find_resource_hierarchies_scattered ([(10, 32)], [h0], [2])
# []
#find_resource_hierarchies_scattered ([(1, 32)], [h0,h1], [2,1])
#- : Interval.interval list = [{b = 1; e = 16}; {b = 17; e = 32}]
