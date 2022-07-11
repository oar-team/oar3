# coding: utf-8
from procset import ProcSet

from oar.kao.scheduling import schedule_id_jobs_ct, set_slots_with_prev_scheduled_jobs
from oar.kao.slot import Slot, SlotSet
from oar.lib import config
from oar.lib.job_handling import JobPseudo
from oar.lib.plugins import find_plugin_function

config["LOG_FILE"] = ":stderr:"

ASSIGN_ENTRY_POINTS = "oar.assign_func"
FIND_ENTRY_POINTS = "oar.find_func"


def set_assign_func(job, name):

    job.assign = True
    job.assign_func = find_plugin_function(ASSIGN_ENTRY_POINTS, name)


def set_find_func(job, name):

    job.find = True
    job.find_func = find_plugin_function(FIND_ENTRY_POINTS, name)


def compare_slots_val_ref(slots, v):
    sid = 1
    i = 0
    while True:
        slot = slots[sid]
        (b, e, itvs) = v[i]
        if (slot.b != b) or (slot.e != e) or not (slot.itvs == itvs):
            return False
        sid = slot.next
        if sid == 0:
            break
        i += 1
    return True


def test_assign_default():

    v = [(0, 59, ProcSet(*[(17, 32)])), (60, 100, ProcSet(*[(1, 32)]))]

    res = ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("node", 2)], res)])],
    )

    set_assign_func(j1, "default")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_find_default():

    v = [(0, 59, ProcSet(*[(17, 32)])), (60, 100, ProcSet(*[(1, 32)]))]

    res = ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("node", 2)], res)])],
    )

    set_find_func(j1, "default")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_assign_one_time_find_default():

    v = [(0, 59, ProcSet(*[(17, 32)])), (60, 100, ProcSet(*[(1, 32)]))]

    res = ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("node", 2)], res)])],
    )

    set_assign_func(j1, "one_time_find")
    set_find_func(j1, "default")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_assign_one_time_find_mld_default():

    v = [(0, 59, ProcSet(*[(17, 32)])), (60, 100, ProcSet(*[(1, 32)]))]

    res = ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("node", 2)], res)])],
    )

    set_assign_func(j1, "one_time_find_mld")
    set_find_func(j1, "default")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_find_contiguous_1h():

    res = ProcSet(*[(1, 32)])
    prop = ProcSet(*[(1, 8), (17, 32)])

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}

    hy = {"resource_id": [[(i, i)] for i in range(1, 32)]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("resource_id", 10)], prop)])],
    )

    set_find_func(j1, "contiguous_1h")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    print(str(j1.res_set))

    assert j1.res_set == ProcSet(*[(17, 26)])


def test_find_contiguous_sorted_1h():

    res = ProcSet(*[(1, 32)])
    prop = ProcSet(*[(1, 2), (4, 12), (14, 22), (24, 29)])

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}

    hy = {"resource_id": [[(i, i)] for i in range(1, 32)]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("resource_id", 5)], prop)])],
    )

    set_find_func(j1, "contiguous_sorted_1h")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    print(str(j1.res_set))

    assert j1.res_set == ProcSet(*[(24, 28)])


def test_find_local():

    v = [(0, 59, ProcSet(*[(1, 8), (13, 32)])), (60, 100, ProcSet(*[(1, 32)]))]

    res = ProcSet(*[(1, 32)])
    prop = ProcSet(*[(6, 32)])

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {}
    hy["resource_id"] = [ProcSet(i) for i in range(1, 32)]
    hy["node"] = [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={})
    j1.simple_req([("node", 1), ("resource_id", 4)], 60, prop)

    #               mld_res_rqts=[
    #    (1, 60,
    #     [([("node", 1), ("resource_id", 4)], prop)]
    #     )
    # ])

    set_find_func(j1, "local")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)
    print("j1.res_set: ", str(j1.res_set))
    ss.show_slots()
    assert j1.res_set == ProcSet(*[(9, 12)])
    assert compare_slots_val_ref(ss.slots, v) is True


def test_find_local_1():

    """Test if find_local assigns resource from node with less available resources"""

    v = [(0, 59, ProcSet(*[(1, 8), (13, 32)])), (60, 100, ProcSet(*[(1, 32)]))]

    res = ProcSet(*[(1, 32)])
    prop = ProcSet(
        *[(1, 12), (15, 32)]
    )  # First node entirely available, second partially available

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {}
    hy["resource_id"] = [ProcSet(i) for i in range(1, 32)]
    hy["node"] = [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("node", 1), ("resource_id", 4)], prop)])],
    )

    set_find_func(j1, "local")

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)
    print("j1.res_set: ", str(j1.res_set))
    ss.show_slots()
    assert j1.res_set == ProcSet(*[(9, 12)])
    assert compare_slots_val_ref(ss.slots, v) is True


def test_assign_one_time_find_contiguous_1h():

    res = ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}

    hy = {}
    hy["resource_id"] = [ProcSet(i) for i in range(1, 32)]
    hy["node"] = [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]

    j1 = JobPseudo(
        id=1,
        start_time=5,
        walltime=10,
        res_set=ProcSet(*[(10, 20)]),
        types={},
        ts=False,
        ph=0,
    )

    set_slots_with_prev_scheduled_jobs(all_ss, [j1], 10)

    j2 = JobPseudo(
        id=2,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("resource_id", 16)], ProcSet(*[(1, 32)]))])],
    )

    set_assign_func(j2, "one_time_find")
    set_find_func(j2, "contiguous_1h")

    schedule_id_jobs_ct(all_ss, {2: j2}, hy, [2], 20)

    print(j2.res_set)
    ss.show_slots()
    assert j2.res_set == ProcSet(*[(1, 9), (21, 27)])


def test_assign_one_time_find_mld_contiguous_1h():

    res = ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}

    hy = {}
    hy["resource_id"] = [ProcSet(i) for i in range(1, 32)]
    hy["node"] = [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]

    j1 = JobPseudo(
        id=1,
        start_time=5,
        walltime=10,
        res_set=ProcSet(*[(10, 20)]),
        types={},
        ts=False,
        ph=0,
    )

    set_slots_with_prev_scheduled_jobs(all_ss, [j1], 10)

    j2 = JobPseudo(
        id=2,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[
            (1, 60, [([("resource_id", 16)], ProcSet(*[(1, 32)]))]),
            (2, 60, [([("resource_id", 16)], ProcSet(*[(1, 32)]))]),
        ],
    )

    set_assign_func(j2, "one_time_find_mld")
    set_find_func(j2, "contiguous_1h")

    schedule_id_jobs_ct(all_ss, {2: j2}, hy, [2], 20)

    print(j2.res_set)
    ss.show_slots()
    assert j2.res_set == ProcSet(*[(1, 9), (21, 27)])


def test_find_contiguous_sorted_1h_2():

    res = ProcSet(*[(1, 32)])

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), 0, 1000))
    all_ss = {"default": ss}

    hy = {"resource_id": [[(i, i)] for i in range(1, 32)]}

    j2 = JobPseudo(
        id=2,
        start_time=5,
        walltime=10,
        res_set=ProcSet(*[(10, 20)]),
        types={},
        ts=False,
        ph=0,
    )

    set_slots_with_prev_scheduled_jobs(all_ss, [j2], 10)

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("resource_id", 16)], res)])],
    )

    set_find_func(j1, "contiguous_sorted_1h")
    ss.show_slots()
    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    print(j1.res_set)

    assert j1.res_set == ProcSet(*[(1, 16)])


def test_find_begin():

    res = ProcSet(*[(1, 62)])
    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), 0, 200))
    all_ss = {"default": ss}
    hy = {"resource_id": [ProcSet(i) for i in range(1, 33)]}

    j1 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(1, 60, [([("resource_id", 32)], res)])],
    )

    j2 = JobPseudo(
        id=1,
        types={},
        deps=[],
        key_cache={},
        mld_res_rqts=[(2, 60, [([("resource_id", 32)], res)])],
    )

    set_find_func(j1, "begin")
    set_find_func(j2, "begin")
    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    print(j1.res_set)
    print(j2.res_set)

    assert j1.res_set == ProcSet(*[(1, 32)])
    assert j2.res_set == ProcSet(*[(1, 16)])
