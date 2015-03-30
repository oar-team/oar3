# -*- coding: utf-8 -*-
from sqlalchemy.orm.util import object_state

from oar.lib import Resource
from oar.lib import db


def test_db_insert():
    r1 = Resource(id=100)
    db.add(r1)
    assert object_state(r1).pending is True
    db.flush()
    assert object_state(r1).persistent is True
    db.commit()


def test_db_query():
    res = db.query(Resource).order_by(Resource.id.asc()).first()
    assert res.id == 100
