# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.kao.job import JobPseudo
from oar.kao.slot import Slot, SlotSet
from oar.kao.interval import equal_itvs
from oar.kao.scheduling import (assign_resources_mld_job_split_slots,
                                schedule_id_jobs_ct,
                                set_slots_with_prev_scheduled_jobs)
from oar.lib import config, get_logger

config['LOG_FILE'] = '/dev/stdout'
logger = get_logger("oar.test")


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


def test_quotas_one_job_no_rules():
    config['QUOTAS'] = 'yes'

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

    assert compare_slots_val_ref(ss.slots, v)


def test():
    """
       $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'*'} = [-1, -1, -1] ;

    Examples:
    
    - No more than 100 resources used by 'john' at a time:
    
    $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, -1, -1] ;

    - No more than 100 resources used by 'john' and no more than 4 jobs at a
    time:
    
    $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, 4, -1] ;
    
   - No more than 150 resources used by jobs of besteffort type at a time:
    
    $Gantt_quotas->{'*'}->{'*'}->{'besteffort'}->{'*'} = [150, -1, -1] ;
    
    - No more than 150 resources used and no more than 35 jobs of besteffort
    type at a time:

    $Gantt_quotas->{'*'}->{'*'}->{'besteffort'}->{'*'} = [150, 35, -1] ;
    
    - No more than 200 resources used by jobs in the project "proj1" at a
    time:
    
       $Gantt_quotas->{'*'}->{'proj1'}->{'*'}->{'*'} = [200, -1, -1] ;
    
    - No more than 20 resources used by 'john' in the project 'proj12' at a
    time:
    
       $Gantt_quotas->{'*'}->{'proj12'}->{'*'}->{'john'} = [20, -1, -1] ;
    
    - No more than 80 resources used by jobs in the project "proj1" per user
    at a time:
    
       $Gantt_quotas->{'*'}->{'proj1'}->{'*'}->{'/'} = [80, -1, -1] ;
   
   - No more than 50 resources used per user per project at a time:
    
    $Gantt_quotas->{'*'}->{'/'}->{'*'}->{'/'} = [50, -1, -1] ;
    
    - No more than 200 resource hours used per user at a time:
    
    $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'/'} = [-1, -1, 200] ;
    """
    pass
