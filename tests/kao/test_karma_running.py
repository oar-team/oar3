# coding: utf-8
"""Regression tests for get_running_consumption (fairsharing + running jobs).

The function computes, for every job in ``Running`` state inside a window, the
resource-seconds already used and the resource-seconds reserved (asked)::

    used  = max(0, window_stop - max(start, window_start)) * nb_resources
    asked = max(0, min(start + walltime, window_stop) - max(start, window_start))
            * nb_resources

It returns the same 6-tuple shape as the accounting helpers:
``(sum_asked, sum_used, projects_asked, projects_used, users_asked, users_used)``.
"""

import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.kao.karma import get_running_consumption
from oar.lib.database import ephemeral_session
from oar.lib.models import Job, Resource

from ..helpers import insert_running_jobs


@pytest.fixture(scope="function", autouse=True)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)

    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


def _create_resources(session, nb_resources):
    for _ in range(nb_resources):
        Resource.create(session, network_address="localhost")


def _set_start_time(session, jid, start_time):
    session.query(Job).filter(Job.id == jid).update(
        {Job.start_time: start_time}, synchronize_session=False
    )
    session.commit()


def test_get_running_consumption(minimal_db_initialization):
    session = minimal_db_initialization

    # 2 resources per job (insert_running_jobs assigns resources[i:i+2])
    _create_resources(session, 4)

    jids = insert_running_jobs(
        session, nb_jobs=2, user="alice", project="proj", walltime=400
    )
    assert len(jids) == 2

    _set_start_time(session, jids[0], 1500)
    _set_start_time(session, jids[1], 800)

    result = get_running_consumption(session, ["default"], 1000, 2000)

    assert result == (
        1200.0,
        3000.0,
        {"proj": 1200.0},
        {"proj": 3000.0},
        {"alice": 1200.0},
        {"alice": 3000.0},
    )

    # Clamping: inside [2000, 2100] both reservations (start + walltime) are
    # over, so nothing is "asked"; the two jobs are still running, so the
    # elapsed "used" is (2100 - 2000) * 2 resources = 200 each.
    clamped = get_running_consumption(session, ["default"], 2000, 2100)
    assert clamped[0] == 0.0  # sum_asked
    assert clamped[1] == 400.0  # sum_used
    assert all(v == 0.0 for v in clamped[2].values())  # projects_asked
    assert clamped[3] == {"proj": 400.0}  # projects_used
    assert all(v == 0.0 for v in clamped[4].values())  # users_asked
    assert clamped[5] == {"alice": 400.0}  # users_used


def test_get_running_consumption_empty(minimal_db_initialization):
    session = minimal_db_initialization

    _create_resources(session, 4)

    result = get_running_consumption(session, ["default"], 1000, 2000)

    assert result == (0.0, 0.0, {}, {}, {}, {})
