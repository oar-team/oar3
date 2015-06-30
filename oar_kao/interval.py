from itertools import islice


def ordered_ids2itvs(ids):
    itvs = []
    if ids:
        b = ids[0]
        e = ids[0]
        for i in ids:
            if i > (e + 1):  # end itv and prepare new itv
                itvs.append((b, e))
                b = i
            e = i
        itvs.append((b, e))

    return itvs


def unordered_ids2itvs(unordered_ids):
    return ordered_ids2itvs(sorted(unordered_ids))


def itvs2ids(itvs):
    ids = []
    for itv in itvs:
        b, e = itv
        ids.extend(range(b, e + 1))

    return ids


def test_and_sub_prefix_itvs(prefix_itvs, itvs):
    # need of sub_intervals
    flag = True
    lx = len(prefix_itvs)
    ly = len(itvs)
    i = 0
    residue_itvs = []

    if (lx > ly):
        return (False, residue_itvs)

    if (lx == 0):
        return (True, itvs)

    while (flag) and (i < lx) and (i < ly):
        x = prefix_itvs[i]
        y = itvs[i]
        i += 1
        if not (x[0] == y[0]) and (x[1] == y[1]):
            flag = False
            i -= 1

    residue_itvs = [item for item in islice(itvs, i, None)]

    return flag, residue_itvs

# test_and_sub_prefix_itvs( [(1,4),(6,9)], [(6,9),(10,17),(20,30)] )
# (false, [{b = 10; e = 17}; {b = 20; e = 30}])

# test_and_sub_prefix_itvs( [(1,4),(6,9)], [(1,4),(6,9),(10,17),(20,30)] )
# (false, [{b = 10; e = 17}; {b = 20; e = 30}])


def equal_itvs2ids(itvs1, itvs2):
    rids1 = itvs2ids(itvs1)
    rids2 = itvs2ids(itvs2)
    return (rids1 == rids2)

def equal_itvs(itvs1, itvs2):

    lx = len(itvs1)
    ly = len(itvs2)

    if (lx == 0) and (lx == 0):
        return True

    ix = 0
    iy = 0
    next_x = True
    next_y = True

    while True:

        if next_x:
            if ix == lx:
                return False
            x = itvs1[ix]
        if next_y:
            if iy == ly:
                return False
            y = itvs2[iy]

        if x[0] != y[0]:

            return False

        if x[1] == y[1]:
            if (ix == (lx-1)) and (iy == (ly-1)):
                return True

            ix += 1
            iy += 1
            next_x = True
            next_y = True

        elif x[1] > y[1]:
            x = (y[1]+1, x[1])
            iy += 1
            next_x = False
            next_y = True
        else: #x[1] < y[1]
            y = (x[1]+1, y[1])
            ix += 1
            next_x = True
            next_y = False

# suppose same segmentation (be careful 2 itvs with differents segmentation can be equal)
# [(1,20)] == [(1,10), (11,15), (16,20)]
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
    lx = len(itvs1)
    ly = len(itvs2)
    i = 0
    k = 0
    itvs = []

    while (i < lx) and (lx > 0):
        x = itvs1[i]
        if (k == ly):
            itvs.append(x)
            i += 1
        else:
            y = itvs2[k]
            # y before x w/ no overlap
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
                            itvs.append((x[0], y[0] - 1))
                            itvs1[i] = (y[1] + 1, x[1])
                            k += 1
                        else:
                            # x overlap partially
                            itvs.append((x[0], y[0] - 1))
                            i += 1
                    else:
                        if (y[1] < x[1]):
                            # x overlap partially
                            itvs1[i] = (y[1] + 1, x[1])
                            k += 1
                        else:
                            # y overlap totally x
                            i += 1

    return itvs


def add_intervals(itvs_x, itvs_y):
    lx = len(itvs_x)
    ly = len(itvs_y)
    ix = 0
    iy = 0
    itvs = []
    o_itvs = False

    if itvs_x == []:
        return itvs_y[:]
    elif itvs_y == []:
        return itvs_x[:]

    while (ix < lx) or (iy < ly):
        if (ix < lx):
            x = itvs_x[ix]
        if (iy < ly):
            y = itvs_y[iy]

        # print "intermediate", itvs
        # x,y no overlap
        if x[1] < y[0] - 1:  # x before
            if (ix < lx):
                itvs.append(x)
                ix += 1
            else:
                itvs.append(y)
                iy += 1

        elif (x[1] == (y[0] - 1)) and (ix < lx):  # contiguous itvs
            itvs.append((x[0], y[1]))
            ix += 1
            iy += 1

        elif y[1] < x[0] - 1:  # y before
            if (iy < ly):
                itvs.append(y)
                iy += 1
            else:
                itvs.append(x)
                ix += 1

        elif (y[1] == (x[0] - 1)) and (iy < ly):  # contiguous itvs
            itvs.append((y[0], x[1]))
            iy += 1
            ix += 1

        # x,y overlap
        else:
            if y[0] > x[0]:  # x begin
                a = x[0]
            else:
                a = y[0]

            o_itvs = True
            while o_itvs:
                if y[0] > x[1] or x[0] > y[1]:
                    o_itvs = False
                if y[1] < x[1]:  # x overlaps totally y
                    b = x[1]
                    iy += 1
                    if (iy < ly):
                        y = itvs_y[iy]
                    else:
                        ix += 1
                        o_itvs = False
                else:  # x begins by overlap y
                    b = y[1]
                    ix += 1
                    if (ix < lx):
                        x = itvs_x[ix]
                    else:
                        iy += 1
                        o_itvs = False

            itvs.append((a, b))

    return itvs


def intersec(itvs1, itvs2):
    lx = len(itvs1)
    ly = len(itvs2)
    i = 0
    k = 0
    itvs = []

    while (i < lx) and (lx > 0) and (ly > 0):
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
                        itvs.append((y[0], x[1]))
                        i += 1
                else:
                    if (y[1] <= x[1]):
                        itvs.append((x[0], y[1]))
                        k += 1
                    else:
                        itvs.append(x)
                        i += 1

    return itvs


def itvs_size(itvs):
    size = 0
    for itv in itvs:
        size += itv[1] - itv[0] + 1
    return size
