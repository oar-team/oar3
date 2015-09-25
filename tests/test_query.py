# -*- coding: utf-8 -*-
import os
import pytest

from oar.lib.models import all_models, all_tables
from oar.lib.fixture import load_fixtures
from oar.lib import db


REFTIME = 1437050120


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        here = os.path.abspath(os.path.dirname(__file__))
        filename = os.path.join(here, "data", "dataset_1.json")
        load_fixtures(db, filename, ref_time=REFTIME, clear=False)
        yield


def test_simple_models():
    expected_models = [
        'Accounting', 'AdmissionRule', 'AssignedResource', 'Challenge',
        'EventLog', 'EventLogHostname', 'File', 'FragJob',
        'GanttJobsPrediction', 'GanttJobsPredictionsLog',
        'GanttJobsPredictionsVisu', 'GanttJobsResource',
        'GanttJobsResourcesLog', 'GanttJobsResourcesVisu', 'Job',
        'JobDependencie', 'JobResourceDescription', 'JobResourceGroup',
        'JobStateLog', 'JobType', 'MoldableJobDescription', 'Queue',
        'Resource', 'ResourceLog', 'Scheduler'
    ]
    assert set(list(db.models.keys())) == set(expected_models)
    assert set(list(dict(all_models()).keys())) == set(expected_models)

    # len(tables) = len(Models) + table schema
    assert len(dict(all_tables()).keys()) == len(expected_models) + 1


def test_get_jobs_for_user_query():
    jobs = db.queries.get_jobs_for_user("user1").all()
    assert len(jobs) == 2
    assert jobs[0].id == 5
    assert jobs[1].id == 6
    jobs = db.queries.get_jobs_for_user("user1",
                                        from_time=jobs[0].submission_time,
                                        to_time=jobs[0].submission_time).all()
    assert len(jobs) == 1
    assert jobs[0].id == 5
    assert jobs[0].state == 'Waiting'
