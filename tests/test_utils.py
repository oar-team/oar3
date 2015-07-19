# -*- coding: utf-8 -*-
import time
import random

import pytest
import datetime

from collections import OrderedDict
from decimal import Decimal

from oar.lib import Database
from oar.lib.utils import (SimpleNamespace, cached_property, ResultProxyIter,
                           try_convert_decimal, row2dict, merge_dicts,
                           get_table_name, render_query, to_json, Command)
from oar.lib.compat import is_pypy
from . import assert_raises


@pytest.fixture
def db(request, monkeypatch):
    db = Database(uri='sqlite://')

    table = db.Table(
        'table',
        db.Column('num', db.Integer),
        db.Column('word', db.String(255))
    )

    class Model(db.Model):
        id = db.Column(db.Integer, primary_key=True)

    db.create_all()
    db.session.execute(table.insert(), ({'num': 1, 'word': 'one'},
                                        {'num': 2, 'word': 'two'},
                                        {'num': 3, 'word': 'three'},
                                        {'num': 4, 'word': 'four'}))
    for _ in range(10):
        db.session.add(Model())
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

    assert isinstance(MyClass.value, cached_property)
    myobject = MyClass()
    value = myobject.value
    assert myobject.value == value
    assert myobject._cache['value'] == myobject.value
    if is_pypy:
        return
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


def test_render_query(db):
    query = db['Model'].query.order_by(db['Model'].id)

    try:
        import sqlparse  # noqa
        expected_sql = "\nSELECT model.id\nFROM model\nORDER BY model.id;"
    except:
        expected_sql = "\nSELECT model.id\nFROM model ORDER BY model.id;"

    def assert_sql_query(expected_sql, query):
        expected_parts = []
        for part in expected_sql.split('\n'):
            if part:
                expected_parts.append(part.strip())
        parts = []
        for part in query.split('\n'):
            if part:
                parts.append(part.strip())
        for part in parts:
            assert part in expected_parts

    assert render_query(query) == query.render()
    assert_sql_query(expected_sql, render_query(query))
    assert_sql_query(expected_sql, repr(query.render()))


def test_try_convert_decimal():
    assert try_convert_decimal("3.1") == 3.1
    assert try_convert_decimal("3.10") == 3.1
    assert try_convert_decimal("3") == 3
    assert try_convert_decimal("3,2") == "3,2"
    assert try_convert_decimal("foo") == "foo"


def test_merge_dicts():
    dict_a = dict(a="A")
    dict_b = dict(b="B")
    assert dict(a="A", b="B") == merge_dicts(dict_a, dict_b)
    assert id(merge_dicts(dict_a, dict_b)) != id(merge_dicts(dict_a, dict_b))

    dicto_a = dict(a=dict_a)
    dicto_b = dict(b=dict_b)
    dicto_merge = merge_dicts(dicto_a, dicto_b)
    assert dicto_merge == dict(a=dict(a="A"), b=dict(b="B"))
    dicto_a['a']['a'] = 'AA'
    dicto_b['b']['b'] = 'BB'
    assert dicto_merge == dict(a=dict(a="AA"), b=dict(b="BB"))


def test_get_table_name():
    assert get_table_name('Actor') == 'actor'
    assert get_table_name('TvShow') == 'tv_show'
    assert get_table_name('MySuperTable') == 'my_super_table'
    assert get_table_name('my_super_table') == 'my_super_table'
    assert get_table_name('OAR') == 'OAR'
    assert get_table_name('OARLib') == 'oar_lib'


def test_to_json():
    a = OrderedDict()
    a['name'] = "Monkey D. Luffy"
    a['birthday'] = datetime.datetime(2015, 7, 19, 9, 14, 22, 140921)
    a['level'] = 90
    a['length'] = Decimal('177.85')
    a['parents'] = OrderedDict()
    a['parents']["father"] = dict(name="Monkey D. Dragon")
    a['parents']["mother"] = dict(name="Unknown")

    class FakeDict(object):
        def to_dict(self):
            return a

    expected_json = """
{
    "name": "Monkey D. Luffy",
    "birthday": "2015-07-19T09:14:22.140921",
    "level": 90,
    "length": "177.85",
    "parents": {
        "father": {
            "name": "Monkey D. Dragon"
        },
        "mother": {
            "name": "Unknown"
        }
    }
}
""".strip()

    assert to_json(a) == expected_json
    assert to_json(FakeDict()) == expected_json

    with assert_raises(TypeError, "<SimpleObject> is not JSON serializable"):
        class SimpleObject(object):
            def __repr__(self):
                return "<SimpleObject>"
        assert to_json(SimpleObject()) == expected_json


def test_command():
    assert Command("false").run() == 1
    assert Command("true").run() == 0
    cmd = Command("sleep 0.02; echo hello; echo world 1>&2")
    assert cmd(timeout=1) == 0
    assert cmd.stdout == "hello\n"
    assert cmd.stderr == "world\n"
    assert cmd(timeout=0.01) != 0
