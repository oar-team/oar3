# coding: utf-8
"""Tests for :func:`oar.lib.resource.procset_from_values`.

``ProcSet(*values)`` merges its arguments one by one and is quadratic when the
values are scattered, which stalls the scheduler on job constraints matching
many non-contiguous resources.
"""

import random

from procset import ProcSet

from oar.lib.resource import procset_from_values


def test_procset_from_values_equivalence():
    random.seed(0)
    for _ in range(500):
        n = random.randint(0, 80)
        values = [random.randint(0, 50) for _ in range(n)]
        assert procset_from_values(values) == ProcSet(*values)


def test_procset_from_values_edge_cases():
    assert procset_from_values([]) == ProcSet()
    assert procset_from_values([7]) == ProcSet(7)
    # duplicates and adjacent runs must collapse without overlapping intervals
    assert procset_from_values([1, 2, 3, 5, 6, 6, 7, 9]) == ProcSet(1, 2, 3, 5, 6, 6, 7, 9)


def test_procset_from_values_large_scattered():
    values = list(range(0, 20000, 2))
    result = procset_from_values(values)
    assert len(result) == len(values)
    assert list(result.intervals()) == [(2 * i, 2 * i) for i in range(len(values))]
    assert 0 in result
    assert 1 not in result
    assert 19998 in result
