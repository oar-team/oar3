# coding: utf-8
from oar.lib.hierarchy import find_resource_hierarchies_scattered


def test_find_resource_hierarchies_scattere1():
    h0 = [[(1, 16)], [(17, 32)]]

    x = find_resource_hierarchies_scattered([(1, 32)], [h0], [2])
    assert x == [(1, 32)]


def test_find_resource_hierarchies_scattere2():
    h0 = [[(1, 16)], [(17, 32)]]
    h1 = [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]

    x = find_resource_hierarchies_scattered([(1, 32)], [h0, h1], [2, 1])
    assert x == [(1, 8), (17, 24)]


def test_find_resource_hierarchies_scattere3():
    h0 = [[(1, 16)], [(17, 32)]]
    h1 = [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]

    x = find_resource_hierarchies_scattered(
        [(1, 12), (17, 28)], [h0, h1], [2, 1])
    assert x == [(1, 8), (17, 24)]


def test_find_resource_hierarchies_scattere4():
    h0 = [[(1, 16)], [(17, 32)]]
    h1 = [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]
    h2 = [[(1, 4)], [(5, 8)], [(8, 12)], [(13, 16)],
          [(17, 20)], [(21, 24)], [(25, 28)], [(29, 32)]]

    x = find_resource_hierarchies_scattered([(1, 32)], [h0, h1, h2], [2, 1, 1])
    assert x == [(1, 4), (17, 20)]
