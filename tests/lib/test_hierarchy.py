# coding: utf-8
from procset import ProcSet

from oar.lib.hierarchy import (
    _bottom_map,
    _children_map,
    extract_all_best_half_scattered_block_itv,
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


def test_extract_all_best_half_scattered_block_itv_all_1():
    ALL = -1
    y = [ProcSet(*z) for z in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]
    a = extract_all_best_half_scattered_block_itv(ProcSet((1, 32)), y, ALL)
    assert a == ProcSet((1, 32))


def test_extract_all_best_half_scattered_block_itv_all_2():
    ALL = -1
    y = [ProcSet(*z) for z in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]
    a = extract_all_best_half_scattered_block_itv(ProcSet((2, 32)), y, ALL)
    assert a == ProcSet()


def test_extract_all_best_half_scattered_block_itv_best_1():
    BEST = -2
    y = [ProcSet(*z) for z in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]
    a = extract_all_best_half_scattered_block_itv(ProcSet((2, 32)), y, BEST)
    assert a == ProcSet(((9, 32)))


def test_extract_all_best_half_scattered_block_itv_half_best_1():
    HALF_BEST = -3
    y = [ProcSet(*z) for z in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]
    a = extract_all_best_half_scattered_block_itv(ProcSet((2, 32)), y, HALF_BEST)
    assert a == ProcSet(((9, 16)))


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


# ---------------------------------------------------------------------------
# Regression tests for the hierarchy parent/children cache
# (oar.lib.hierarchy._children_map / _bottom_map).  They pin the exact
# behaviour of find_resource_n_h, including non-tree hierarchies.
# ---------------------------------------------------------------------------


def test_children_map_returns_subsets():
    parent = [ProcSet((1, 16)), ProcSet((17, 32))]
    child = [ProcSet((1, 8)), ProcSet((9, 16)), ProcSet((17, 24)), ProcSet((25, 32))]
    mapping = _children_map(parent, child)
    assert mapping[id(parent[0])] == [child[0], child[1]]
    assert mapping[id(parent[1])] == [child[2], child[3]]


def test_bottom_map_keeps_intersection_for_overlapping_blocks():
    # child (5,12) overlaps parent (1,8) without being a subset: the original
    # code kept parent & child == (5,8); _bottom_map must do the same.
    parent = [ProcSet((1, 8)), ProcSet((9, 16))]
    child = [ProcSet((5, 12)), ProcSet((1, 8)), ProcSet((9, 16))]
    mapping = _bottom_map(parent, child)
    assert mapping[id(parent[0])] == [ProcSet((5, 8)), ProcSet((1, 8))]
    assert mapping[id(parent[1])] == [ProcSet((9, 12)), ProcSet((9, 16))]


def test_find_resource_scattered_non_tree_overlap():
    # Non-tree hierarchy: the bottom level must keep the intersection semantics.
    parent = [ProcSet((1, 8)), ProcSet((9, 16))]
    child = [ProcSet((5, 12)), ProcSet((1, 8)), ProcSet((9, 16))]
    assert find_resource_hierarchies_scattered(
        ProcSet((1, 32)), [parent, child], [1, 1]
    ) == ProcSet((5, 8))
    assert find_resource_hierarchies_scattered(
        ProcSet((6, 12)), [parent, child], [1, 1]
    ) == ProcSet((9, 12))


def test_find_resource_scattered_4_level_tree():
    core = [ProcSet((i, i)) for i in range(1, 33)]
    cpu = [ProcSet((8 * c + 1, 8 * c + 8)) for c in range(4)]
    node = [ProcSet((16 * n + 1, 16 * n + 16)) for n in range(2)]
    model = [ProcSet((1, 32))]
    assert find_resource_hierarchies_scattered(
        ProcSet((1, 32)), [model, node, cpu, core], [1, 1, 1, 8]
    ) == ProcSet((1, 8))


def test_find_resource_scattered_duplicate_levels():
    # the finest level repeated (core == resource_id style) must behave like the
    # original code, including for a shared level list object
    core = [ProcSet((i, i)) for i in range(1, 33)]
    cpu = [ProcSet((8 * c + 1, 8 * c + 8)) for c in range(4)]
    assert (
        find_resource_hierarchies_scattered(
            ProcSet((1, 32)), [cpu, core, core], [1, 2, 2]
        )
        == ProcSet()
    )
    assert (
        find_resource_hierarchies_scattered(ProcSet((1, 32)), [core, core], [4, 2])
        == ProcSet()
    )


def test_cache_not_stale_between_different_hierarchies():
    # two different hierarchies evaluated in the same process must not share a
    # stale cached map; a repeated call must stay consistent
    ha0 = [ProcSet((1, 16))]
    ha1 = [ProcSet((1, 8)), ProcSet((9, 16))]
    hb0 = [ProcSet((1, 32))]
    hb1 = [ProcSet((1, 16)), ProcSet((17, 32))]
    assert find_resource_hierarchies_scattered(
        ProcSet((1, 16)), [ha0, ha1], [1, 1]
    ) == ProcSet((1, 8))
    assert find_resource_hierarchies_scattered(
        ProcSet((1, 32)), [hb0, hb1], [1, 1]
    ) == ProcSet((1, 16))
    assert find_resource_hierarchies_scattered(
        ProcSet((1, 16)), [ha0, ha1], [1, 1]
    ) == ProcSet((1, 8))
