# -*- coding: utf-8 -*-
from oar.lib.utils import SimpleNamespace


def test_simple_namespace():
    namespace = SimpleNamespace(a="a", b="b")
    assert namespace.a == "a"
    assert namespace.b == "b"
    assert namespace['b'] == "b"
    assert namespace['a'] == "a"
    assert dict(namespace) == namespace.__dict__
