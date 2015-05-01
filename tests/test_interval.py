from oar.kao.interval import (intersec, extract_n_scattered_block_itv,
                              ordered_ids2itvs, itvs2ids, add_intervals,
                              equal_itvs)


def test_intersec():
    x = [(1, 4), (6, 9)]
    y = intersec(x, x)
    y == x


def test_extract_n_scattered_block_itv_1():
    y = [[(1, 4), (6, 9)], [(10, 17)], [(20, 30)]]
    a = extract_n_scattered_block_itv([(1, 30)], y, 3)
    a == [(1, 4), (6, 17), (20, 30)]


def test_extract_n_scattered_block_itv_2():
    y = [[(1, 4), (10, 17)], [(6, 9), (19, 22)], [(25, 30)]]
    a = extract_n_scattered_block_itv([(1, 30)], y, 2)
    a == [(1, 4), (6, 9), (10, 17), (19, 22)]


def test_ordered_ids2itvs():
    y = [1, 3, 4, 5, 7, 10, 11, 12, 23]
    r = [(1, 1), (3, 5), (7, 7), (10, 12), (23, 23)]
    a = ordered_ids2itvs(y)
    a == r


def test_itvs2ids():
    y = [(1, 1), (3, 5), (7, 7), (10, 12), (23, 23)]
    r = [1, 3, 4, 5, 7, 10, 11, 12, 23]
    a = itvs2ids(y)
    a == r


def test_add_intervals1():
    r = [(1, 4), (6, 9)]
    x = [(1, 4)]
    y = [(6, 9)]
    a = add_intervals(x, y)
    a == r


def test_add_intervals2():
    r = [(1, 9)]
    x = [(1, 4), (6, 9)]
    y = [(2, 7)]
    a = add_intervals(x, y)
    a == r


def test_add_intervals3():
    r = [(1, 30)]
    x = [(3, 4), (6, 7), (9, 17), (19, 30)]
    y = [(1, 10), (15, 20), (22, 24)]
    a = add_intervals(x, y)
    a == r

def test_equal_itvs1():
    x = [(1,10), (11,15), (16,20)]
    y = [(1,10), (11,15), (16,20)]
    equal_itvs(x,y)

def test_equal_itvs2():
    x = [(1,10), (11,15), (16,20)]
    y = [(1,10), (11,20)]
    equal_itvs(x,y)
