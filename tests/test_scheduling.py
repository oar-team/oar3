# coding: utf-8
from oar.kao.job import JobPseudo
from oar.kao.slot import Slot, SlotSet
from oar.kao.interval import equal_itvs
from oar.kao.scheduling import (assign_resources_mld_job_split_slots,
                                schedule_id_jobs_ct,
                                set_slots_with_prev_scheduled_jobs)
from oar.lib import config, get_logger

import pdb

config['LOG_FILE'] = '/dev/stdout'
log = get_logger("oar.test")


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


def test_set_slots_with_prev_scheduled_jobs_1():
    v = [(1, 4, [(1, 32)]),
         (5, 14, [(1, 9), (21, 32)]),
         (15, 29, [(1, 32)]),
         (30, 49, [(1, 4), (16, 19), (29, 32)]),
         (50, 100, [(1, 32)])
         ]

    j1 = JobPseudo(id=1, start_time=5, walltime=10,
                   res_set=[(10, 20)], types={}, ts=False, ph=0)
    j2 = JobPseudo(id=2, start_time=30, walltime=20, res_set=[
                   (5, 15), (20, 28)], types={}, ts=False, ph=0)

    ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 100))
    all_ss = {"default": ss}

    set_slots_with_prev_scheduled_jobs(all_ss, [j1, j2], 10)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_assign_resources_mld_job_split_slots():

    v = [(0, 59, [(17, 32)]), (60, 100, [(1, 32)])]

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    # j1 = JobPseudo(id=1, start_time=0, walltime=0, types={},
    # key_cache="",
    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    assign_resources_mld_job_split_slots(ss, j1, hy, -1)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_schedule_id_jobs_ct_1():
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
    ], ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v) is True


def test_schedule_container1():

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={"container": ""}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={"inner": "1"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 30, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 10)

    j2.res_set, [(1, 8)]


def test_schedule_container_error1():

    res = [(1, 32)]
    res2 = [(17, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={"container": ""}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 2)], res)])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={"inner": "1"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 30, [([("node", 1)], res2)])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    assert j2.start_time == -1


def test_schedule_container_error2():
    ''' inner exceeds container's capacity'''

    res = [(1, 32)]

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={"container": ""}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={"inner": "1"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 20, [([("node", 3)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    assert j2.start_time == -1


def test_schedule_container_error3():
    ''' inner exceeds time container's capacity'''

    res = [(1, 32)]

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={"container": ""}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={"inner": "1"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 70, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    assert j2.start_time == -1


def test_schedule_container_prev_sched():

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={"container": ""}, deps=[], key_cache={},
                   res_set=[(7, 27)],
                   start_time=200,
                   walltime=150,
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={"inner": "1"}, deps=[], key_cache={},
                   res_set=[(9, 16)],
                   start_time=210,
                   walltime=70,
                   mld_res_rqts=[(1, 30, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    j3 = JobPseudo(id=3, types={"inner": "1"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 30, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    set_slots_with_prev_scheduled_jobs(all_ss, [j1, j2], 20)

    schedule_id_jobs_ct(all_ss, {3: j3}, hy, [3], 20)

    assert j3.start_time == 200
    assert j3.res_set == [(17, 24)]


def test_schedule_container_recursif():

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1,
                   types={"container": ""},
                   deps=[],
                   key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2,
                   types={"container": "", "inner": "1"},
                   deps=[],
                   key_cache={},
                   mld_res_rqts=[(1, 50, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j3 = JobPseudo(id=2,
                   types={"inner": "2"},
                   deps=[],
                   key_cache={},
                   mld_res_rqts=[(1, 30, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2, 3: j3}, hy, [1, 2, 3], 10)

    assert j3.res_set == [(1, 8)]


def test_schedule_container_prev_sched_recursif():

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1,
                   types={"container": ""},
                   deps=[], key_cache={},
                   res_set=[(7, 27)],
                   start_time=200,
                   walltime=150,
                   ts=False, ph=0)

    j2 = JobPseudo(id=2,
                   types={"container": "", "inner": "1"},
                   deps=[],
                   key_cache={},
                   res_set=[(15, 25)],
                   start_time=210,
                   walltime=70,
                   ts=False, ph=0)

    j3 = JobPseudo(id=3, types={"inner": "2"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 30, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    set_slots_with_prev_scheduled_jobs(all_ss, [j1, j2], 20)

    schedule_id_jobs_ct(all_ss, {3: j3}, hy, [3], 20)

    assert j3.start_time == 210
    assert j3.res_set == [(17, 24)]


def test_schedule_w_temporally_fragmented_container():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 5000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1,
                   types={"container": "yop"},
                   deps=[], key_cache={},
                   res_set=[(7, 32)],
                   start_time=200,
                   walltime=50,
                   ts=False, ph=0)

    j2 = JobPseudo(id=2,
                   types={"container": "yop"},
                   deps=[],
                   key_cache={},
                   res_set=[(15, 25)],
                   start_time=1000,
                   walltime=200,
                   ts=False, ph=0)

    j3 = JobPseudo(id=3, types={"inner": "yop"}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 100, [([("node", 1)], res[:])])],
                   ts=False, ph=0)

    set_slots_with_prev_scheduled_jobs(all_ss, [j1, j2], 20)
    all_ss['yop'].show_slots()

    schedule_id_jobs_ct(all_ss, {3: j3}, hy, [3], 20)

    all_ss['yop'].show_slots()

    assert j3.start_time == 1000
    assert j3.res_set == [(17, 24)]


def test_simple_dependency():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={}, deps=[(1,"Waiting",0)], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)
    
    assert j2.start_time == 60

    
def test_error_dependency():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    j2 = JobPseudo(id=2, types={}, deps=[(1,"Error",0)], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    assert (not hasattr(j2, "start_time"))
    
def test_terminated_dependency():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j2 = JobPseudo(id=2, types={}, deps=[(1,"Terminated",0)], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {2: j2}, hy, [2], 20)

    assert j2.start_time == 0
    

def test_schedule_placeholder1():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    #job type: placeholder="yop"
    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 4)], res[:])])],
                   ts=False, ph=1, ph_name="yop")

    j2 = JobPseudo(id=2, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 50, [([("node", 4)], res[:])])],
                   ts=False, ph=0)
    
    #Allow type: allow="yop"
    j3 = JobPseudo(id=3, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 4)], res[:])])],
                   ts=False, ph=2, ph_name="yop")

    
    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2, 3: j3}, hy, [1, 2, 3], 20)

    print "Placeholder: j1.start_time:", j1.start_time, " j2.start_time:", j2.start_time,\
        " j3.start_time:", j3.start_time
        
    
    assert (j2.start_time == 80) and (j3.start_time == 0)
   

def test_schedule_placeholder2():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=0)

    #job type: allow="yop"
    j2 = JobPseudo(id=2, types={}, deps=[(1,"Waiting",0)], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 2)], res[:])])],
                   ts=False, ph=2, ph_name="yop")

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)
    
    print "j1.start_time:", j1.start_time, " j2.start_time:", j2.start_time

    assert  j2.start_time == 60

def test_schedule_placeholder_prev_sched():

    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   res_set=[(7, 27)],
                   start_time=200,
                   walltime=150,
                   mld_res_rqts=[(1, 60, [([("node", 2)], res[:])])],
                   ts=False, ph=1, ph_name="yop")

    j2 = JobPseudo(id=2, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 150, [([("node", 4)], res[:])])],
                   ts=False, ph=0)

    j3 = JobPseudo(id=3, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 500, [([("node", 2)], res[:])])],
                   ts=False, ph=2, ph_name="yop")

    #pdb.set_trace()
    set_slots_with_prev_scheduled_jobs(all_ss, [j1], 20)

    all_ss['default'].show_slots()
    #pdb.set_trace()

    schedule_id_jobs_ct(all_ss, {2: j2, 3: j3}, hy, [2,3], 20)

    print "j1.start_time:", j1.start_time, "j2.start_time:", j2.start_time,\
        " j3.start_time:", j3.start_time
    
    all_ss['default'].show_slots()
    #pdb.set_trace()
    
    #assert j3.start_time == 150
    assert j3.res_set == [(1, 16)]


def test_schedule_timesharing1():
    res = [(1, 32)]
    ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 60, [([("node", 4)], res[:])])],
                   user="toto", name="yop",
                   ts=True, ts_user="*", ts_name="*", ph=0)

    j2 = JobPseudo(id=2, types={}, deps=[], key_cache={},
                   mld_res_rqts=[(1, 80, [([("node", 4)], res[:])])],
                   user="toto", name="yop",
                   ts=True, ts_user="*", ts_name="*", ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    print "j1.start_time:", j1.start_time, " j2.start_time:", j2.start_time

    assert j1.start_time == j2.start_time
    
def test_schedule_timesharing2():
    pass


def test_schedule_timesharing_prev_sched():
    pass
