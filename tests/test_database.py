# -*- coding: utf-8 -*-
from oar.lib.models import Resource


def test_deferred_reflection():
    resource = Resource.query.order_by(Resource.id).first()
    keys = resource.asdict().keys()
    assert "cpu" in keys
    assert "core" in keys
    assert "mem" in keys
    assert "host" in keys
