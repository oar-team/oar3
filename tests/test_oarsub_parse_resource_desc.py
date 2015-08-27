# coding: utf-8
from __future__ import unicode_literals, print_function

from oar.cli.oarsub import parse_resource_descriptions

default_res = '/resource_id=1'
nodes_res = 'resource_id'


def test_parse_resource_descriptions_1():
    str_res_req = ['/resource_id=1']
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [([{u'property': u'',
                          u'resources': [{u'resource': u'resource_id', u'value': '1'}]}],
                        None)]


def test_parse_resource_descriptions_2():
    str_res_req = []
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [([{u'property': u'',
                          u'resources': [{u'resource': u'resource_id', u'value': '1'}]}],
                        None)]


def test_parse_resource_descriptions_3():
    str_res_req = ['/switch=2/nodes=10,walltime=10:0']
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [([{u'property': u'',
                          u'resources': [{u'resource': u'switch', u'value': '2'},
                                         {u'resource': 'resource_id', u'value': '10'}]}],
                        36000)]


def test_parse_resource_descriptions_4():
    str_res_req = ["{gpu='YES'}/nodes=ALL+{gpu='NO'}/core=20"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [([{u'property': u"gpu='YES'",
                          u'resources': [{u'resource': 'resource_id', u'value': -1}]},
                         {u'property': u"gpu='NO'",
                          u'resources': [{u'resource': u'core', u'value': '20'}]}],
                        None)]


def test_parse_resource_descriptions_5():
    str_res_req = ["{gpu='YES'}/nodes=ALL", "{gpu='NO'}/core=20"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [([{u'property': u"gpu='YES'",
                          u'resources': [{u'resource': 'resource_id', u'value': -1}]}],
                        None),
                       ([{u'property': u"gpu='NO'",
                          u'resources': [{u'resource': u'core', u'value': '20'}]}],
                        None)]
