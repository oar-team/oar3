# coding: utf-8
from procset import ProcSet
from oar.lib.job_handling import JobPseudo
from oar.kao.slot import Slot, SlotSet
from oar.kao.scheduling_basic import (assign_resources_mld_job_split_slots,
                                      schedule_id_jobs_ct)


def compare_slots_val_ref(slots, v):
    sid = 1
    i = 0
    while True:
        slot = slots[sid]
        (b, e, itvs) = v[i]
        if (slot.b != b) or (slot.e != e) or not (slot.itvs == itvs):
            return False
        sid = slot.next
        if (sid == 0):
            break
        i += 1
    return True


def test_assign_resources_mld_job_split_slots():
    v = [(0, 59, ProcSet(*[(17, 32)])), (60, 100,  ProcSet(*[(1, 32)]))]

    res =  ProcSet(*[(1, 32)])
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    hy = {'node': [ ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]] ]}

    # j1 = JobPseudo(id=1, start_time=0, walltime=0, types={}, key_cache="",
    j1 = JobPseudo(id=1, types={}, key_cache="",
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    assign_resources_mld_job_split_slots(ss, j1, hy)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_schedule_id_jobs_ct_1():
    v = [(0, 59, ProcSet(*[(17, 32)])), (60, 100,  ProcSet(*[(1, 32)]))]
    res =  ProcSet(*[(1, 32)])

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {'default': ss}
    hy = {'node': [ ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]] ]}


    j1 = JobPseudo(id=1, types={}, key_cache="",
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True
