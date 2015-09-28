# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

from codecs import open
from copy import deepcopy
from tempfile import mkstemp
from oar.kao.job import JobPseudo
from oar.kao.slot import Slot, SlotSet
from oar.lib.interval import equal_itvs
from oar.kao.scheduling import (schedule_id_jobs_ct,
                                set_slots_with_prev_scheduled_jobs)
import oar.kao.quotas as qts
import oar.lib.resource as rs

from oar.lib import config, get_logger
# import pdb

config['LOG_FILE'] = '/dev/stdout'
logger = get_logger("oar.test")

"""
    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+
"""


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


@pytest.fixture(scope='module', autouse=True)
def oar_conf(request):
    config['QUOTAS'] = 'yes'

    def remove_quotas():
        config['QUOTAS'] = 'no'

    request.addfinalizer(remove_quotas)


@pytest.fixture(scope='function', autouse=True)
def reset_quotas():
    qts.quotas_rules = {}
    qts.quotas_job_types = ['*']


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
    rs.default_resource_itvs = deepcopy(res)

    ss = SlotSet(Slot(1, 0, 0, deepcopy(res), 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, queue='default', user='toto', project='')
    j1.simple_req(('node', 2), 60, res)

    schedule_id_jobs_ct(all_ss, {1: j1}, hy, [1], 20)

    print(j1.start_time)
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


def test_quotas_four_jobs_rule_1():

    config['QUOTAS'] = 'yes'
    # quotas.set_quotas_rules({('*', '*', '*', '/'): [1, -1, -1]})
    # global quotas_rules
    qts.quotas_rules = {('*', '*', '*', '/'): [16, -1, -1],
                        ('*', 'yop', '*', '*'): [-1, 1, -1]}

    res = [(1, 32)]
    rs.default_resource_itvs = deepcopy(res)

    ss = SlotSet(Slot(1, 0, 0, deepcopy(res), 0, 10000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, start_time=0, walltime=20,
                   queue='default', user='toto', project='',
                   res_set=[(9, 24)], types={}, ts=False, ph=0)
    j2 = JobPseudo(id=2, start_time=0, walltime=50,
                   queue='default', user='lulu', project='yop',
                   res_set=[(1, 8)])

    j3 = JobPseudo(id=3, queue='default', user='toto', project='')
    j3.simple_req(('node', 1), 10, res)

    j4 = JobPseudo(id=4, queue='default', user='lulu', project='yop')
    j4.simple_req(('node', 1), 60, res)

    set_slots_with_prev_scheduled_jobs(all_ss, [j1, j2], 5)

    ss.show_slots()
    # pdb.set_trace()
    schedule_id_jobs_ct(all_ss, {3: j3, 4: j4}, hy, [3, 4], 5)

    print(j3.start_time, j4.start_time)

    assert j3.start_time == 20
    assert j3.res_set == [(9, 16)]
    assert j4.start_time == 50
    assert j4.res_set == [(1, 8)]


def test_quotas_three_jobs_rule_1():

    config['QUOTAS'] = 'yes'
    # quotas.set_quotas_rules({('*', '*', '*', '/'): [1, -1, -1]})
    # global quotas_rules
    qts.quotas_rules = {('*', '*', '*', '/'): [16, -1, -1],
                        ('default', '*', '*', '*'): [-1, -1, 2000]}

    res = [(1, 32)]
    rs.default_resource_itvs = deepcopy(res)

    ss = SlotSet(Slot(1, 0, 0, deepcopy(res), 0, 10000))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, start_time=50, walltime=100,
                   queue='default', user='toto', project='',
                   res_set=[(17, 24)], types={}, ts=False, ph=0)

    j2 = JobPseudo(id=2, queue='default', user='toto', project='')
    j2.simple_req(('node', 1), 200, res)

    j3 = JobPseudo(id=3, queue='default', user='lulu', project='yop')
    j3.simple_req(('node', 1), 100, res)

    set_slots_with_prev_scheduled_jobs(all_ss, [j1], 5)

    ss.show_slots()
    # pdb.set_trace()
    schedule_id_jobs_ct(all_ss, {2: j2, 3: j3}, hy, [2, 3], 5)

    print(j2.start_time, j3.start_time)

    assert j2.start_time == 150
    assert j2.res_set == [(1, 8)]
    assert j3.start_time == 0
    assert j3.res_set == [(1, 8)]


def test_quotas_two_job_rules_nb_res_quotas_file():

    config['QUOTAS'] = 'yes'
    _, quotas_file_name = mkstemp()
    config['QUOTAS_FILE'] = quotas_file_name

    # quotas_file = open(quotas_file_name, 'w')
    with open(config['QUOTAS_FILE'], 'w', encoding="utf-8") as quotas_fd:
        quotas_fd.write('{"quotas": {"*,*,*,toto": [1,-1,-1],"*,*,*,john": [150,-1,-1]}}')

    qts.load_quotas_rules()

    res = [(1, 32)]
    rs.default_resource_itvs = res

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

    j2 = JobPseudo(id=2, types={}, deps=[], key_cache={},
                   queue='default', user='tutu', project='',
                   mld_res_rqts=[
        (1, 60,
         [([("node", 2)], res)]
         )
    ], ts=False, ph=0)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    assert j1.res_set == []
    assert j2.res_set == [(1, 16)]


def test_quotas_two_jobs_job_type_proc():
    config['QUOTAS'] = 'yes'
    _, quotas_file_name = mkstemp()
    config['QUOTAS_FILE'] = quotas_file_name

    # quotas_file = open(quotas_file_name, 'w')
    with open(config['QUOTAS_FILE'], 'w', encoding="utf-8") as quotas_fd:
        quotas_fd.write('{"quotas": {"*,*,yop,*": [-1,1,-1]}, "quotas_job_types": ["yop"]}')

    qts.load_quotas_rules()

    print(qts.quotas_rules, qts.quotas_job_types)

    res = [(1, 32)]
    rs.default_resource_itvs = res

    ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
    all_ss = {"default": ss}
    hy = {'node': [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]}

    j1 = JobPseudo(id=1, queue='default', user='toto', project='', types={'yop'})
    j1.simple_req(('node', 1), 50, res)
    j2 = JobPseudo(id=2, queue='default', user='toto', project='', types={'yop'})
    j2.simple_req(('node', 1), 50, res)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    print(j1.start_time, j2.start_time)

    assert j1.start_time == 0
    assert j2.start_time == 50
