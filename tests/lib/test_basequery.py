# coding: utf-8
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from oar.lib.basequery import BaseQueryCollection
from oar.lib.database import ephemeral_session
from oar.lib.job_handling import insert_job
from oar.lib.models import Job


@pytest.fixture(scope="function", autouse=False)
def minimal_db_initialization(request, setup_config):
    _, engine = setup_config
    session_factory = sessionmaker(bind=engine)
    scoped = scoped_session(session_factory)
    with ephemeral_session(scoped, engine, bind=engine) as session:
        yield session


def _get_jobs(session, user, job_ids):
    return BaseQueryCollection(session).get_jobs_for_user(
        user, None, None, None, job_ids, None, None, detailed=True
    )


def test_get_jobs_for_user_explicit_id_waiting(minimal_db_initialization):
    session = minimal_db_initialization
    jid = insert_job(
        session, res=[(60, [("resource_id=1", "")])], properties="", user="titi"
    )
    session.commit()
    ids = [j.id for j in _get_jobs(session, "titi", [jid]).all()]
    assert ids == [jid]


def test_get_jobs_for_user_explicit_id_killed_while_waiting(minimal_db_initialization):
    # A job killed before being launched: never got an AssignedResource, is
    # absent from the gantt visu, and has a non-zero stop_time.  "oarstat -fj"
    # used to print nothing for such a job (it was dropped from the q1|q2|q3
    # union of filter_jobs_for_user).
    session = minimal_db_initialization
    jid = insert_job(
        session, res=[(60, [("resource_id=1", "")])], properties="", user="titi"
    )
    session.query(Job).filter(Job.id == jid).update(
        {Job.state: "Error", Job.stop_time: 123456, Job.assigned_moldable_job: 0},
        synchronize_session=False,
    )
    session.commit()
    ids = [j.id for j in _get_jobs(session, "titi", [jid]).all()]
    assert ids == [jid]
