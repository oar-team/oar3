# -*- coding: utf-8 -*-
import time
import random

import pytest

from collections import OrderedDict

from oar.lib import Database
from oar.lib.utils import (SimpleNamespace, cached_property, ResultProxyIter,
                           try_convert_decimal, row2dict)


@pytest.fixture
def db(request, monkeypatch):
    db = Database(uri='sqlite://')

    table = db.Table(
        'table',
        db.Column('num', db.Integer),
        db.Column('word', db.String(255))
    )
    db.create_all()
    db.session.execute(table.insert(), ({'num': 1, 'word': 'one'},
                                        {'num': 2, 'word': 'two'},
                                        {'num': 3, 'word': 'three'},
                                        {'num': 4, 'word': 'four'}))

    db.commit()
    return db


def test_simple_namespace():
    namespace = SimpleNamespace(a="a", b="b")
    assert namespace.a == "a"
    assert namespace.b == "b"
    assert namespace['b'] == "b"
    assert namespace['a'] == "a"
    assert dict(namespace) == namespace.__dict__


def test_cached_propery():
    class MyClass(object):

        @cached_property
        def value(self):
            return "%x-%s" % (random.getrandbits(32), int(time.time()))

    myobject = MyClass()
    value = myobject.value
    assert myobject.value == value
    assert myobject._cache['value'] == myobject.value
    del myobject.value
    assert 'value' not in myobject._cache
    assert myobject.value != value


def test_row2dict(db):
    item = db.session.execute(db['table'].select().limit(1)).fetchall()[0]
    assert set(item.keys()) == {'num', 'word'}
    assert set(row2dict(item).keys()) == {'num', 'word'}
    assert isinstance(row2dict(item), OrderedDict)
    assert set(row2dict(item, ignore_keys=('num',)).keys()) == {'word'}
    assert row2dict(item, ignore_keys=('num', 'word')) == OrderedDict()


def test_result_proxy_iter(db):
    result_proxy = ResultProxyIter(db.session.execute(db['table'].select()))
    assert len(result_proxy) == 4

    # result_proxy is an iterator
    assert len(list(result_proxy)) == 4
    # reset iterator automatically
    assert len(list(result_proxy)) == 4

    for item in result_proxy:
        assert isinstance(item, OrderedDict)
        assert set(item.keys()) == {'num', 'word'}


def test_try_convert_decimal():
    assert try_convert_decimal("3.1") == 3.1
    assert try_convert_decimal("3.10") == 3.1
    assert try_convert_decimal("3") == 3
    assert try_convert_decimal("3,2") == "3,2"
    assert try_convert_decimal("foo") == "foo"
