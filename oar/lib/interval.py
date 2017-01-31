# coding: utf-8
from __future__ import print_function, absolute_import, unicode_literals
from interval_set import interval_set

from itertools import islice


def ordered_ids2itvs(ids):
    return interval_set.id_list_to_iterval_set(ids)


def unordered_ids2itvs(unordered_ids):
    return ordered_ids2itvs(sorted(unordered_ids))


def itvs2ids(itvs):
    return interval_set.interval_set_to_id_list(itvs)


def itvs2batsim_str(itvs):
    return interval_set.interval_set_to_string(itvs, separator=',')


def itvs2batsim_str0(itvs):
    batsim_str = ''
    for itv in itvs:
        b, e = itv
        if b == e:
            batsim_str += "%d," % (b - 1)
        else:
            batsim_str += "%d-%d," % (itv[0] - 1, itv[1] - 1)
    return batsim_str.rstrip(',')


def batsim_str2itvs(batsim_str):
    return interval_set.string_to_interval_set(batsim_str, separator=',')


def equal_and_sub_prefix_itvs(prefix_itvs, itvs):
    # need of sub_intervals
    lx = len(prefix_itvs)
    ly = len(itvs)
    i = 0
    residue_itvs = []

    if (lx > ly):
        return (False, [])

    if (lx == 0):
        return (True, itvs)

    while True:
        x = prefix_itvs[i]
        y = itvs[i]
        i += 1
        if (x[0] != y[0]) or (x[1] != y[1]):
            return (False, [])
        if (i == lx):
            break
        if (i == ly):
            return(False, [])

    residue_itvs = [item for item in islice(itvs, i, None)]

    return True, residue_itvs

# test_and_sub_prefix_itvs( [(1,4),(6,9)], [(6,9),(10,17),(20,30)] )
# (false, [{b = 10; e = 17}; {b = 20; e = 30}])

# test_and_sub_prefix_itvs( [(1,4),(6,9)], [(1,4),(6,9),(10,17),(20,30)] )
# (false, [{b = 10; e = 17}; {b = 20; e = 30}])


def equal_itvs2ids(itvs1, itvs2):
    rids1 = itvs2ids(itvs1)
    rids2 = itvs2ids(itvs2)
    return (rids1 == rids2)


def equal_itvs(itvs1, itvs2):
    return interval_set.equals(itvs1, itvs2)


def equal_itvs_same_segmentation(itvs1, itvs2):
    lx = len(itvs1)
    ly = len(itvs2)
    i = 0
    if (lx != ly):
        return False
    if (lx == 0):
        return True
    while (i < lx):
        x = itvs1[i]
        y = itvs2[i]
        if not ((x[0] == y[0]) and (x[1] == y[1])):
            return False
        i += 1
    return True


def extract_n_scattered_block_itv(itvs1, itvs_ref, n):
    # itv_l_a lst_itvs_reference n
    # need of test_and_sub_prefix_itvs:
    lr = len(itvs_ref)
    i = 0
    itvs = []

    while (n > 0) and (i < lr):
        x = itvs_ref[i]
        y = intersec(itvs1, x)
        if equal_itvs(x, y):
            # test if itvs[-1] and x[0] are in contact
            if itvs and ((itvs[-1][1] + 1) == x[0][0]):
                itvs[-1] = (itvs[-1][0], x[0][1])
                itvs.extend(x[1:])
            else:
                itvs.extend(x)
            n -= 1
        i += 1

    if (n == 0):
        itvs.sort()
        return itvs
    else:
        return []

# y = [ [(1, 4), (6,9)],  [(10,17)], [(20,30)] ]
# extract_n_scattered_block_itv([(1,30)], y, 3)
# [(1, 4), (6, 9), (10, 17), (20, 30)]

# extract_n_scattered_block_itv ([(1, 12), (15,32)], y, 2)

# y = [ [(1, 4), (10,17)],  [(6,9), (19,22)], [(25,30)] ]


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
        if (intersec(x, itvs) != []):
            r_itvss.append(x)
        i += 1
    return r_itvss


def sub_intervals(itvs1, itvs2):
    return interval_set.difference(itvs1, itvs2)


def add_intervals(itvs_x, itvs_y):
    return interval_set.union(itvs_x, itvs_y)


def intersec(itvs1, itvs2):
    return interval_set.intersection(itvs1, itvs2)


def itvs_size(itvs):
    return interval_set.total(itvs)


def aggregate_itvs(itvs):
    return interval_set.aggregate(itvs)
