# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.kao.slot import Slot, SlotSet
from oar.kao.interval import equal_itvs
from oar.kao.scheduling import (schedule_id_jobs_ct)
from oar.kao.job import JobPseudo

from oar.lib import config
config['LOG_FILE'] = '/dev/stdout'


def set_assign_func(job, name):
    import oar.kao.advanced_scheduling
    job.assign = True
    job.assign_func = getattr(oar.kao.advanced_scheduling, 'assign_' + name)


def set_find_func(job, name):
    import oar.kao.advanced_scheduling
    job.find = True
    job.find_func = getattr(oar.kao.advanced_scheduling, 'find_' + name)


def compare_slots_val_ref(slots, v):
    sid = 1
    i = 0
    while True:
        slot = slots[sid]
        (b, e, itvs) = v[i]
        if ((slot.b != b) or (slot.e != e)
                or not equal_itvs(slot.itvs, itvs)):
            return False
        sid = slot.next
        if (sid == 0):
            break
        i += 1
    return True


def test_assign_default():

    v = [(0, 59, [(17, 32)]), (60, 100, [(1, 32)])]

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ])

    set_assign_func(j1, 'default')

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_find_default():

    v = [(0, 59, [(17, 32)]), (60, 100, [(1, 32)])]

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ])

    set_find_func(j1, 'default')

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_assign_one_time_find_default():

    v = [(0, 59, [(17, 32)]), (60, 100, [(1, 32)])]

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ])

    set_assign_func(j1, 'one_time_find')
    set_find_func(j1, 'default')

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_find_contiguous_1h():

    res = [(1, 32)]
    prop = [(1, 8), (17, 32)]

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}

    hy = {'resource_id': [[(i, i)] for i in range(1, 32)]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[
        (1, 60,
         [([("resource_id", 10)], prop)]
         )
    ])

    set_find_func(j1, 'contiguous_1h')

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    print(j1.res_set)

    assert j1.res_set == [(17, 26)]


def test_find_contiguous_sorted_1h():

    res = [(1, 32)]
    prop = [(1, 2), (4, 12), (14, 22), (24, 29)]

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}

    hy = {'resource_id': [[(i, i)] for i in range(1, 32)]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[
        (1, 60,
         [([("resource_id", 5)], prop)]
         )
    ])

    set_find_func(j1, 'contiguous_sorted_1h')

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    print(j1.res_set)

    assert j1.res_set == [(24, 28)]
