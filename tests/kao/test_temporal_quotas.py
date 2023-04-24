# coding: utf-8
import time
from copy import deepcopy
from datetime import datetime, timedelta

import pytest
from procset import ProcSet

from oar.kao.quotas import Calendar, Quotas
from oar.kao.scheduling import schedule_id_jobs_ct
from oar.kao.slot import Slot, SlotSet
from oar.lib.globals import init_oar
from oar.lib.job_handling import JobPseudo
from oar.lib.logging import get_logger
from oar.lib.resource import ResourceSet
from oar.lib.tools import local_to_sql

config, _, log = init_oar()

config["LOG_FILE"] = ":stderr:"
logger = get_logger(log, "oar.test")

"""
    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+
"""

rules_example_full = {
    "periodical": [
        ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
        ["19:00-00:00 mon-thu * *", "quotas_night", "nights of workdays"],
        ["00:00-08:00 tue-fri * *", "quotas_night", "nights of workdays"],
        ["19:00-00:00 fri * *", "quotas_weekend", "weekend"],
        ["* sat-sun * *", "quotas_weekend", "weekend"],
        ["00:00-08:00 mon * *", "quotas_weekend", "weekend"],
    ],
    "oneshot": [
        ["2020-07-23 19:30", "2020-08-29 8:30", "quotas_holiday", "summer holiday"],
        ["2020-03-16 19:30", "2020-05-10 8:30", "quotas_holiday", "confinement"],
    ],
    "quotas_workday": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
    "quotas_night": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
    "quotas_weekend": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
    "quotas_holiday": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
}

rules_example_simple = {
    "periodical": [
        ["* mon-wed * *", "quotas_1", "test1"],
        ["* thu-sun * *", "quotas_2", "test2"],
    ],
    "quotas_1": {"*,*,*,/": [16, -1, -1], "*,projA,*,*": [20, -1, -1]},
    "quotas_2": {"*,*,*,/": [24, -1, -1], "*,projB,*,*": [15, -1, -1]},
}

rules_default_example = {
    "periodical": [
        ["*,*,*,*", "quotas_night_weekend", "workdays"],
        ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
    ],
    "quotas_workday": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
    "quotas_night_weekend": {"*,*,*,john": [100, -1, -1], "*,projA,*,*": [200, -1, -1]},
}

rules_only_default_example = {
    "periodical": [
        ["*,*,*,*", "quotas_workday", "workdays"],
    ],
    "quotas_workday": {
        "*,*,*,john": [100, -1, -1],
    },
}


def add_oneshot_to_simple_example():
    example_w_oneshot = deepcopy(rules_example_simple)

    tw = period_weekstart()
    t = tw + int(1.5 * 86400)
    t0 = tw + int(86400 / 2)
    t1 = t0 + int(2.5 * 86400)
    example_w_oneshot["oneshot"] = [
        [
            local_to_sql(t0)[:-3],
            local_to_sql(t1)[:-3],
            "quotas_holiday",
            "summer holiday",
        ]
    ]
    example_w_oneshot["quotas_holiday"] = {
        "*,*,*,*": [32, -1, -1],
    }
    return (example_w_oneshot, t, tw, t0, t1)


def compare_slots_val_ref(slots, v):
    sid = 1
    i = 0
    while True:
        slot = slots[sid]
        (b, e, itvs) = v[i]
        if (slot.b != b) or (slot.e != e) or not slot.itvs == itvs:
            return False
        sid = slot.next
        if sid == 0:
            break
        i += 1
    return True


@pytest.fixture(scope="module", autouse=True)
def oar_conf(request):
    config["QUOTAS"] = "yes"
    # config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks

    def remove_quotas():
        config["QUOTAS"] = "no"
        Quotas.enabled = False
        Quotas.calendar = None

    request.addfinalizer(remove_quotas)


@pytest.fixture(scope="function", autouse=True)
def reset_quotas():
    Quotas.enabled = False
    Quotas.calendar = None
    Quotas.default_rules = {}
    Quotas.job_types = ["*"]


def period_weekstart():
    t_dt = datetime.fromtimestamp(time.time()).date()
    t_weekstart_day_dt = t_dt - timedelta(days=t_dt.weekday())
    return int(datetime.combine(t_weekstart_day_dt, datetime.min.time()).timestamp())


def test_calendar_periodical_fromJson():
    calendar = Calendar(rules_example_full)
    print()
    calendar.show()
    check, periodical_id = calendar.check_periodicals()
    print(check, periodical_id)
    assert check


def test_calendar_periodical_default_fromJson():
    calendar = Calendar(rules_default_example)
    print()
    calendar.show()
    check, periodical_id = calendar.check_periodicals()
    print(check, periodical_id)
    assert check


def test_calendar_periodical_only_default_fromJson():
    calendar = Calendar(rules_only_default_example)
    print()
    calendar.show()
    check, periodical_id = calendar.check_periodicals()
    print(check, periodical_id)
    # import pdb; pdb.set_trace()
    assert check


def test_calendar_periodical_fromJson_bad():
    assert True
    #    pass
    # ["09:00-19:00 mon-fri * *", "quotas_workday", "workdays"],


def test_calendar_rules_at_1():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    Quotas.calendar.show()
    t0 = period_weekstart()

    quotas_rules_id, remaining_period = Quotas.calendar.rules_at(t0)

    assert quotas_rules_id == 0
    assert remaining_period == 259200


def test_calendar_rules_at_2():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True

    (example_w_oneshot, t, _, _, _) = add_oneshot_to_simple_example()

    Quotas.calendar = Calendar(example_w_oneshot)
    Quotas.calendar.show(t)

    quotas_rules_id, remaining_period = Quotas.calendar.rules_at(t)

    assert quotas_rules_id == 2
    assert remaining_period == 129600


def test_calendar_simple_slotSet_1():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    ss = SlotSet(Slot(1, 0, 0, res, t0, t0 + 3 * 86400))
    print(ss)
    assert ss.slots[1].quotas_rules_id == 0


def test_calendar_simple_slotSet_2():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    check, periodical_id = Quotas.calendar.check_periodicals()
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    ss = SlotSet(Slot(1, 0, 0, res, t0, t0 + 4 * 86400))

    print(ss)
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e - ss.slots[1].b == 3 * 86400 - 1


def test_calendar_simple_slotSet_3():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    check, periodical_id = Quotas.calendar.check_periodicals()
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    ss = SlotSet(Slot(1, 0, 0, res, t0, t0 + 5 * 86400))
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e - ss.slots[1].b == 3 * 86400 - 1


def test_calendar_simple_slotSet_4():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2 * 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    print("t0: {} t1: {} t1-t0: {}".format(t0, t1, t1 - t0))
    print(ss)
    v = []
    i = 1
    while i:
        s = ss.slots[i]
        print(
            "Slot: {}  duration: {} quotas_rules_id: {}".format(
                i, s.e - s.b + 1, s.quotas_rules_id
            )
        )
        v.append((i, s.e - s.b + 1, s.quotas_rules_id))
        i = s.next

    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e - ss.slots[1].b == 3 * 86400 - 1
    assert v == [(1, 259200, 0), (2, 345600, 1), (3, 259200, 0), (4, 345600, 1)]


def test_calendar_simple_slotSet_5():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2 * 7 * 86400

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    print("t0: {} t1: {} t1-t0: {}".format(t0, t1, t1 - t0))
    print(ss)
    v = []
    i = 1
    while i:
        s = ss.slots[i]
        print(
            "Slot: {}  duration: {} quotas_rules_id: {}".format(
                i, s.e - s.b + 1, s.quotas_rules_id
            )
        )
        v.append((i, s.e - s.b + 1, s.quotas_rules_id))
        i = s.next

    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e - ss.slots[1].b == 3 * 86400 - 1
    assert v == [
        (1, 259200, 0),
        (2, 345600, 1),
        (3, 259200, 0),
        (4, 345600, 1),
        (5, 1, 0),
    ]


def test_temporal_slotSet_oneshot():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True

    res = ProcSet(*[(1, 32)])
    ResourceSet.default_itvs = ProcSet(*res)

    (rules_example_w_oneshot, t, tw, _, _) = add_oneshot_to_simple_example()

    Quotas.calendar = Calendar(rules_example_w_oneshot)

    Quotas.calendar.show(tw + 3600)

    t1 = tw + 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), tw, t1))

    print(ss)

    print(
        "oneshot t0: {} t1: {}".format(
            Quotas.calendar.oneshots[0][0] - tw, Quotas.calendar.oneshots[0][1] - tw
        )
    )

    v = []
    i = 1
    while i:
        s = ss.slots[i]
        print(
            "Slot: {}  duration: {} quotas_rules_id: {}".format(
                i, s.e - s.b + 1, s.quotas_rules_id
            )
        )
        v.append((i, s.e - s.b + 1, s.quotas_rules_id))
        i = s.next

    assert v == [(1, 43200, 0), (2, 216000, 2), (3, 259200, 0), (4, 86400, 1)]


def test_calendar_simple_slotSet_multi_slot_1():
    assert True


def test_check_slots_quotas_1():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2 * 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    j1 = JobPseudo(
        id=2,
        types={},
        deps=[],
        key_cache={},
        queue="default",
        user="toto",
        project="",
        ts=False,
        ph=0,
    )

    res = Quotas.check_slots_quotas(ss.slots, 1, 4, j1, 2, 7 * 86400)
    print(res)
    assert res == (False, "different quotas rules over job's time", "", 0)


def test_check_slots_quotas_2():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2 * 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    j1 = JobPseudo(
        id=2,
        types={},
        deps=[],
        key_cache={},
        queue="default",
        user="john",
        project="",
        ts=False,
        ph=0,
    )

    res = Quotas.check_slots_quotas(ss.slots, 1, 1, j1, 10, 86400)
    print(res)
    assert res == (True, "quotas ok", "", 0)

    res = Quotas.check_slots_quotas(ss.slots, 1, 1, j1, 20, 86400)
    assert res == (False, "nb resources quotas failed", ("*", "*", "*", "/"), 16)


def test_temporal_quotas_4_jobs_rule_nb_res_1():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    res = ProcSet(*[(1, 32)])
    ResourceSet.default_itvs = ProcSet(*res)

    t0 = period_weekstart()
    t1 = t0 + 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), t0, t1))

    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j1 = JobPseudo(id=1, queue="default", user="toto", project="")
    j1.simple_req(("node", 3), 60, res)

    j2 = JobPseudo(id=2, queue="default", user="toto", project="")
    j2.simple_req(("node", 4), 60, res)

    j3 = JobPseudo(id=3, queue="default", user="toto", project="")
    j3.simple_req(("node", 1), int(3.5 * 86400), res)

    j4 = JobPseudo(id=4, queue="default", user="toto", project="")
    j4.simple_req(("node", 1), 60, res)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2, 3: j3, 4: j4}, hy, [1, 2, 3, 4], 20)

    for j in [j1, j2, j3, j4]:
        print(j.id, j.start_time - t0, j.res_set if hasattr(j, "res_set") else None)
    assert j1.start_time - t0 == 259200
    assert j2.start_time == -1
    assert j3.start_time - t0 == 259260
    assert j4.start_time - t0 == 0

    assert j1.res_set == ProcSet(*[(1, 24)])
    assert j2.res_set == ProcSet()
    assert j3.res_set == ProcSet(*[(1, 8)])
    assert j4.res_set == ProcSet(*[(1, 8)])


def test_temporal_quotas_oneshot_1_job_rule_nb_res_1():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True

    res = ProcSet(*[(1, 32)])
    ResourceSet.default_itvs = ProcSet(*res)

    (rules_example_w_oneshot, t, tw, _, _) = add_oneshot_to_simple_example()

    Quotas.calendar = Calendar(rules_example_w_oneshot)

    t1 = tw + 7 * 86400 - 1
    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), tw, t1))

    print(ss)

    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j = JobPseudo(id=2, queue="default", user="toto", project="")
    j.simple_req(("node", 4), 60, res)

    schedule_id_jobs_ct(all_ss, {1: j}, hy, [1], 20)

    print(
        "oneshot t0: {} t1: {}".format(
            Quotas.calendar.oneshots[0][0] - tw, Quotas.calendar.oneshots[0][1] - tw
        )
    )

    print(j.id, j.start_time - tw, j.res_set if hasattr(j, "res_set") else None)
    assert j.start_time - tw == 43200
    assert j.res_set == ProcSet(*[(1, 32)])


def test_temporal_quotas_job_no_quotas():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True

    res = ProcSet(*[(1, 32)])
    ResourceSet.default_itvs = ProcSet(*res)

    Quotas.calendar = Calendar(rules_example_simple)

    t0 = period_weekstart()
    t1 = t0 + 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), t0, t1))
    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j = JobPseudo(id=2, queue="default", user="toto", project="", no_quotas=True)
    j.simple_req(("node", 4), 60, res)

    schedule_id_jobs_ct(all_ss, {1: j}, hy, [1], 20)

    print(j.id, j.start_time - t0, j.res_set if hasattr(j, "res_set") else None)
    assert j.start_time - t0 == 0
    assert j.res_set == ProcSet(*[(1, 32)])


def test_temporal_quotas_window_time_limit_reached():
    config["QUOTAS_PERIOD"] = 3 * 7 * 86400  # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(rules_example_simple)
    res = ProcSet(*[(1, 32)])
    ResourceSet.default_itvs = ProcSet(*res)

    t0 = period_weekstart()
    t1 = t0 + 7 * 86400 - 1

    ss = SlotSet(Slot(1, 0, 0, ProcSet(*res), t0, t1))

    all_ss = {"default": ss}
    hy = {"node": [ProcSet(*x) for x in [[(1, 8)], [(9, 16)], [(17, 24)], [(25, 32)]]]}

    j1 = JobPseudo(id=1, queue="default", user="toto", project="")
    j1.simple_req(("node", 3), 5 * 86400, res)
    j2 = JobPseudo(id=1, queue="default", user="toto", project="")
    j2.simple_req(("node", 5), 10 * 86400, res)

    schedule_id_jobs_ct(all_ss, {1: j1, 2: j2}, hy, [1, 2], 20)

    assert j1.start_time - t0 == 259200
    assert j2.start_time == -1

    assert j1.res_set == ProcSet(*[(1, 24)])
    assert j2.res_set == ProcSet()
