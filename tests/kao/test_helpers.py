# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.kao.helpers import extract_find_assign_args


def test_extract_find_assign_args():
    raw = "func_find:ip=127.0.0.1:port=13950"
    fname, args, kwargs = extract_find_assign_args(raw)
    assert fname == "func_find"
    assert args == []
    assert kwargs == {'ip': '127.0.0.1', 'port': '13950'}
    raw = "assign_remote:password=passwith==:False:True:=:=="
    fname, args, kwargs = extract_find_assign_args(raw)
    assert fname == "assign_remote"
    assert kwargs == {'password': 'passwith=='}
    assert args == ['False', 'True', '=', '==']
