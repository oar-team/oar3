# coding: utf-8
from __future__ import unicode_literals, print_function

import pytest

from oar.lib import db

from oar.kao.job import insert_job
from oar.kao.kamelot_basic import main


@pytest.fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    db.delete_all()
    db.session.close()


def test_db_kamelot_basic_1():
    # add some resources
    for i in range(5):
        db['Resource'].create(network_address="localhost")

    for i in range(5):
        insert_job(res=[(60, [('resource_id=2', "")])], properties="")

    main()

    req = db['GanttJobsPrediction'].query.all()

    for i, r in enumerate(req):
        print("req:", r.moldable_id, r.start_time)

    assert len(req) == 5
