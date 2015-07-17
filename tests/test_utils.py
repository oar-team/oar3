# -*- coding: utf-8 -*-
import time
import random
from oar.lib.utils import SimpleNamespace, cached_property


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
