# coding: utf-8
from procset import ProcSet


class Hierarchy(object):
    # TODO extract hierarchy from ressources table

    def __init__(self, hy=None, hy_rid=None, ):
        if hy_rid:
            self.hy = {}
            for hy_label, hy_level_roids in hy_rid.items():
                self.hy[hy_label] = [
                    ProcSet(*ids) for k, ids in hy_level_roids.items()]
        else:
            if hy:
                self.hy = hy
            else:
                raise Exception("Hierarchy description must be provided")

def keep_no_empty_scat_bks(itvs, itvss_ref):
    '''
    Keep no empty scattered blocks where their intersection with itvs is not
    empty
    '''
    lr = len(itvss_ref)
    i = 0
    r_itvss = []

    while(i < lr):
        x = itvss_ref[i]
        if len(x & itvs) != 0:
            r_itvss.append(x)
        i += 1
    return r_itvss

def extract_n_scattered_block_itv(itvs1, itvs_ref, n):
    # itv_l_a lst_itvs_reference n
    # need of test_and_sub_prefix_itvs:
    lr = len(itvs_ref)
    i = 0
    itvs = ProcSet()

    while (n > 0) and (i < lr):
        x = itvs_ref[i]
        y = itvs1 & x
        if x == y:
            itvs = itvs | y
            n -= 1
        i += 1

    if (n == 0):
        return itvs
    else:
        return ProcSet()


def find_resource_hierarchies_scattered(itvs, hy, rqts):
    l_hy = len(hy)
    #    print "find itvs: ", itvs, rqts[0]
    if (l_hy == 1):
        return extract_n_scattered_block_itv(itvs, hy[0], rqts[0])
    else:
        return find_resource_n_h(itvs, hy, rqts, hy[0], 0, l_hy)

def find_resource_n_h(itvs, hy, rqts, top, h, h_bottom):

    # potentiel available blocks
    avail_bks = keep_no_empty_scat_bks(itvs, top)
    l_avail_bks = len(avail_bks)

    if (l_avail_bks < rqts[h]):
        # not enough scattered blocks
        return ProcSet()
    else:
        if (h == h_bottom - 2):
            # reach last level hierarchy of requested resource
            # iter on top and find rqts[h-1] block
            itvs_acc = ProcSet()
            i = 0
            nb_r = 0
            while (i < l_avail_bks) and (nb_r != rqts[h]):  # need
                # print avail_bks[i], "*", hy[h+1]
                # TODO test cost of [] filtering .....
                avail_sub_bks = [(avail_bks[i] & x) for x in hy[h + 1] if len(avail_bks[i] & x) != 0]
                # print avail_sub_bks
                # print "--------------------------------------"
                r = extract_n_scattered_block_itv(
                    itvs, avail_sub_bks, rqts[h + 1])
                # r = []
                if (len(r) != 0):
                    # win for this top_block
                    itvs_acc = itvs_acc | r
                    nb_r += 1
                i += 1
            if (nb_r == rqts[h]):
                return itvs_acc
            else:
                return ProcSet()

        else:
            # intermediate hierarchy level
            # iter on available_bk
            itvs_acc = ProcSet()
            i = 0
            nb_r = 0
            while (i < l_avail_bks) and (nb_r != rqts[h]):
                r = find_resource_n_h(
                    itvs, hy, rqts, [avail_bks[i]], h + 1, h_bottom)
                if len(r) != 0:
                    # win for this top_block
                    itvs_acc = itvs_acc | r
                    nb_r += 1
                i += 1
            if (nb_r == rqts[h]):
                return itvs_acc
            else:
                return ProcSet()

# def G(Y):
#    if one h level:
#        extract
#    else:
#        F(X)
#
#
# def F(X):
#    avail_bks
#    return if not enough bks
#    if bottom-1:
#       take (
#        intersec (avail_kbs[i] h+1)
#        extract )
#    else:
#        take ( find(new X new level) )


# h0 = [[(1, 16)],[(17, 32)]]
# h1 = [[(1,8)],[(9,16)],[(17,24)],[(25,32)]]
# h2 = [[(1,4)], [(5,8)], [(8,12)], [(13,16)], [(17,20)],[(21,24)],[(25,28)],[(29,32)]]

# find_resource_hierarchies_scattered ([(1, 32)], [h0,h1,h2], [2,1,1])

# find_resource_hierarchies_scattered ([(1, 32)], [h0], [2])
# [(1, 16), (17, 32)]
# find_resource_hierarchies_scattered ([(10, 32)], [h0], [1])
# [(17, 32)]
# find_resource_hierarchies_scattered ([(10, 32)], [h0], [2])
# []
# find_resource_hierarchies_scattered ([(1, 32)], [h0,h1], [2,1])
# - : Interval.interval list = [{b = 1; e = 16}; {b = 17; e = 32}]
