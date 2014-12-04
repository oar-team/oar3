# -*- coding: utf-8 -*-
from oar import db, Resource


def test_db_query():
    db.reflect()
    db.add(Resource(id=100))
    db.add(Resource(id=200))
    db.flush()
    res = db.query(Resource).order_by(Resource.id.asc()).first()
    assert res.id == 1
