# coding: utf-8
import pytest

from oar.lib import (db, config, EventLog)
from oar.lib.job_handling import (check_end_of_job, insert_job)

import oar.lib.tools  # for monkeypatching

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        yield
        
@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'notify_almighty', lambda x: True)

@pytest.mark.parametrize("error, event_type", [
    (0, 'SWITCH_INTO_TERMINATE_STATE'),
    (1, 'PROLOGUE_ERROR'),
    (2, 'EPILOGUE_ERROR'),
    (3, 'SWITCH_INTO_ERROR_STATE'),
    (5, 'CANNOT_WRITE_NODE_FILE'),
    (6, 'CANNOT_WRITE_PID_FILE'),
    (7, 'USER_SHELL'),
    (8, 'CANNOT_CREATE_TMP_DIRECTORY'),
    (10, 'SWITCH_INTO_ERROR_STATE'),
    (20, 'SWITCH_INTO_ERROR_STATE'),
    (12, 'SWITCH_INTO_ERROR_STATE'),
    (22, 'SWITCH_INTO_ERROR_STATE'),
    (30, 'SSH_TRANSFER_TIMEOUT'),
    (31, 'BAD_HASHTABLE_DUMP'),
    (33, 'SWITCH_INTO_TERMINATE_STATE'),
    (34, 'SWITCH_INTO_TERMINATE_STATE'),
    (50, 'LAUNCHING_OAREXEC_TIMEOUT'),
    (40, 'SWITCH_INTO_TERMINATE_STATE'),
    (42, 'SWITCH_INTO_TERMINATE_STATE'),
    (41, 'SWITCH_INTO_TERMINATE_STATE'),
    (12345, 'EXIT_VALUE_OAREXEC')
])
def test_check_end_of_job(error, event_type):

    config['OAREXEC_DIRECTORY'] = '/tmp/foo'
    job_id = insert_job(res=[(60, [('resource_id=4', '')])], properties='', state='Launching')
    check_end_of_job(job_id, 0, error, ['node1'], 'toto', '/home/toto', None)
    event = db.query(EventLog).first()
    assert event.type ==  event_type

#def job_finishing_sequence
