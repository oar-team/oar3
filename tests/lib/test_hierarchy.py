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
            [(9, 12)],
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


# TODO: 4 level hierarchy
def test_find_resource_hierarchies_scattered5():
    h0 = [ProcSet(*y) for y in [[(1, 32)], [(33, 64)]]]
    h1 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)], [(33, 49)], [(50, 64)]]]
    h2 = [
        ProcSet(*y)
        for y in [
            [(1, 8)],
            [(9, 16)],
            [(17, 24)],
            [(25, 32)],
            [(33, 41)],
            [(42, 49)],
            [(50, 58)],
            [(51, 64)],
        ]
    ]
    h3 = [
        ProcSet(*y)
        for y in [
            [(1, 2)],
            [(3, 4)],
            [(5, 8)],
            [(9, 16)],
            [(10, 12)],
            [(12, 16)],
            [(17, 19)],
            [(20, 22)],
            [(22, 24)],
            [(25, 27)],
            [(28, 30)],
            [(31, 32)],
            [(33, 34)],
            [(35, 37)],
            [(38, 41)],
            [(42, 45)],
            [(46, 47)],
            [(48, 49)],
            [(50, 52)],
            [(53, 54)],
            [(55, 58)],
            [(59, 61)],
            [(62, 63)],
            [(64, 64)],
        ]
    ]

    x = find_resource_hierarchies_scattered(
        ProcSet(*[(1, 64)]), [h0, h1, h2, h3], [2, 2, 1, 1]
    )
    assert x == ProcSet(*[(1, 2), (17, 19), (33, 34), (50, 52)])


# TODO: Tests should pass
def test_find_resource_hierarchies_scattere6_fail():
    h0 = [ProcSet(*y) for y in [[(1, 16)], [(17, 32)]]]
    h1 = [ProcSet(*y) for y in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]
    h2 = [
        ProcSet(*y)
        for y in [
            [(1, 4)],
            [(5, 8)],
            [(9, 12)],
            [(13, 16)],
            [(17, 20)],
            [(21, 24)],
            [(25, 28)],
            [(29, 32)],
        ]
    ]

    x = find_resource_hierarchies_scattered(
        ProcSet(*[(1, 32)]), [h0, h1, h2], [2, 2, 1]
    )

    assert x == ProcSet(*[(1, 4), (9, 12), (17, 20), (25, 28)])

    x = find_resource_hierarchies_scattered(
        ProcSet(*[(1, 32)]), [h0, h1, h2], [1, 2, 1]
    )
    assert x == ProcSet(*[(1, 4), (9, 12)])
