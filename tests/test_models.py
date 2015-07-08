# -*- coding: utf-8 -*-
from sqlalchemy.orm.util import object_state

from oar.lib import db
from oar.lib.models import Resource


def test_resource_select():
    res = db.query(Resource).order_by(Resource.id.asc()).first()
    assert res.id == 1


def test_resource_insert():
    r1 = Resource(id=100000)
    db.add(r1)
    assert object_state(r1).pending is True
    db.flush()
    assert object_state(r1).persistent is True
    db.commit()
