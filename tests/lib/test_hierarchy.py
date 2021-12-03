# coding: utf-8
from procset import ProcSet

from oar.lib.hierarchy import (
    extract_n_scattered_block_itv,
    find_resource_hierarchies_scattered,
    keep_no_empty_scat_bks,
)


def compare_2_lists(a, b):
    for a, b in zip(a, b):
        if a != b:
            return False
    return True


def test_extract_n_scattered_block_itv_1():
    y = [ProcSet(*[(1, 4), (6, 9)]), ProcSet(*[(10, 17)]), ProcSet(*[(20, 30)])]
    a = extract_n_scattered_block_itv(ProcSet((1, 30)), y, 3)
    assert a == ProcSet(*[(1, 4), (6, 17), (20, 30)])


def test_extract_n_scattered_block_itv_2():
    y = [
        ProcSet(*[(1, 4), (10, 17)]),
        ProcSet(*[(6, 9), (19, 22)]),
        ProcSet(*[(25, 30)]),
    ]
    a = extract_n_scattered_block_itv(ProcSet((1, 30)), y, 2)
    assert a == ProcSet(*[(1, 4), (6, 17), (19, 22)])


def test_keep_no_empty_scat_bks():
    y = [ProcSet(*[(1, 4), (6, 9)]), ProcSet(*[(10, 17)]), ProcSet(*[(20, 30)])]
    itvs = ProcSet((1, 15))
    a = keep_no_empty_scat_bks(itvs, y)
    assert compare_2_lists(a, [ProcSet(*[(1, 4), (6, 9)]), ProcSet(*[(10, 17)])])


def test_find_resource_hierarchies_scattere1():
    h0 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)]]]
    itvs = ProcSet((1, 32))
    x = find_resource_hierarchies_scattered(itvs, [h0], [2])
    assert x == itvs


def test_find_resource_hierarchies_scattere2():
    h0 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)]]]
    h1 = [ProcSet(*y) for y in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]

    x = find_resource_hierarchies_scattered(ProcSet(*[(1, 32)]), [h0, h1], [2, 1])
    assert x == ProcSet(*[(1, 8), (17, 24)])


def test_find_resource_hierarchies_scattere3():
    h0 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)]]]
    h1 = [ProcSet(*y) for y in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]

    x = find_resource_hierarchies_scattered(
        ProcSet(*[(1, 12), (17, 28)]), [h0, h1], [2, 1]
    )
    assert x == ProcSet(*[(1, 8), (17, 24)])


def test_find_resource_hierarchies_scattere4():
    h0 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)]]]
    h1 = [ProcSet(*y) for y in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]
    h2 = [
        ProcSet(*y)
        for y in [
            [(1, 4)],
            [(5, 8)],
            [(8, 12)],
            [(13, 16)],
            [(17, 20)],
            [(21, 24)],
            [(25, 28)],
            [(29, 32)],
        ]
    ]

    x = find_resource_hierarchies_scattered(
        ProcSet(*[(1, 32)]), [h0, h1, h2], [2, 1, 1]
    )
    assert x == ProcSet(*[(1, 4), (17, 20)])
