# coding: utf-8
from __future__ import unicode_literals, print_function
from oar.kao.job import JobPseudo
from oar.kao.slot import Slot, SlotSet
from oar.kao.interval import equal_itvs
from oar.kao.scheduling import (assign_resources_mld_job_split_slots,
                                schedule_id_jobs_ct,
                                set_slots_with_prev_scheduled_jobs)
import oar.kao.quotas as qts
import oar.kao.resource as rs

from oar.lib import config, get_logger
import pdb

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
                   queue='default', user='toto', project='',
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert compare_slots_val_ref(ss.slots, v)


def test_quotas_one_job_rule_nb_res_1():
    config['QUOTAS'] = 'yes'
    # quotas.set_quotas_rules({('*', '*', '*', '/'): [1, -1, -1]})
    # global quotas_rules
    qts.quotas_rules = {('*', '*', '*', '/'): [1, -1, -1]}

    res = [(1, 32)]
    rs.default_resource_itvs = res

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=2, types={}, deps=[], key_cache={},
                   queue='default', user='toto', project='',
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert j1.res_set == []


def test_quotas_one_job_rule_nb_res_2():

    config['QUOTAS'] = 'yes'
    # quotas.set_quotas_rules({('*', '*', '*', '/'): [1, -1, -1]})
    # global quotas_rules
    qts.quotas_rules = {('*', '*', '*', '/'): [16, -1, -1]}

    res = [(1, 32)]
    rs.default_resource_itvs = res

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=2, types={}, deps=[], key_cache={},
                   queue='default', user='toto', project='',
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    assert j1.res_set == [(1, 16)]
