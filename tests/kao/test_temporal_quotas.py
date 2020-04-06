# coding: utf-8
import pytest

#from procset import ProcSet

from codecs import open
from copy import deepcopy
from tempfile import mkstemp
from oar.lib.job_handling import JobPseudo
from oar.kao.slot import Slot, SlotSet
from oar.kao.scheduling import (schedule_id_jobs_ct,
                                set_slots_with_prev_scheduled_jobs)
from oar.kao.quotas import Quotas, Calendar
import oar.lib.resource as rs

from oar.lib import config, get_logger

# import pdb

config['LOG_FILE'] = ':stderr:'
logger = get_logger("oar.test")

"""
    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+
"""

json_example_full = {
    "periodical": [
        ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
        ["19:00-00:00 mon-thu * *", "quotas_nigth", "nights of workdays"],
        ["00:00-08:00 tue-fri * *", "quotas_nigth", "nights of workdays"],
        ["19:00-00:00 fri * *", "quotas_weekend", "weekend"],
        ["* sat-sun * *", "quotas_weekend", "weekend"],
        ["00:00-08:00 mon * *", "quotas_weekend", "weekend"]
    ],

    "oneshot": [
        ["2020-07-23 19:30", "2020-08-29 8:30", "quotas_holiday", "summer holiday"],
        ["2020-03-16 19:30", "2020-05-10 8:30", "quotas_holiday", "confinement"]
    ],
    "quotas_workday": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    },
    "quotas_nigth": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    },
    "quotas_weekend": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    },
    "quotas_holiday": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    }
}

def compare_slots_val_ref(slots, v):
    sid = 1
    i = 0
    while True:
        slot = slots[sid]
        (b, e, itvs) = v[i]
        if ((slot.b != b) or (slot.e != e)
                or not slot.itvs == itvs):
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
    Quotas.enabled = False
    Quotas.temporal = False
    Quotas.rules = {}
    Quotas.job_types = ['*']


def test_calendar_periodical_fromJson():

    calendar = Calendar(json_example_full)
    print()
    calendar.show()
    
    check, periodical_id = calendar.check_periodicals()

    print(check, periodical_id)
    #import pdb; pdb.set_trace()
    assert check

    #def test_calendar_periodical_fromJson_bad():
    #    pass
    # ["09:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
