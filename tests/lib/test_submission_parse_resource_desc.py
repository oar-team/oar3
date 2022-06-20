# coding: utf-8
from oar.lib.submission import parse_resource_descriptions

default_res = "/resource_id=1"
nodes_res = "resource_id"


def test_parse_resource_descriptions_1():
    str_res_req = ["/resource_id=1"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [
        (
            [
                {
                    "property": "",
                    "resources": [{"resource": "resource_id", "value": "1"}],
                }
            ],
            None,
        )
    ]


def test_parse_resource_descriptions_2():
    str_res_req = []
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [
        (
            [
                {
                    "property": "",
                    "resources": [{"resource": "resource_id", "value": "1"}],
                }
            ],
            None,
        )
    ]


def test_parse_resource_descriptions_3():
    str_res_req = ["/switch=2/nodes=10,walltime=10:0"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [
        (
            [
                {
                    "property": "",
                    "resources": [
                        {"resource": "switch", "value": "2"},
                        {"resource": "resource_id", "value": "10"},
                    ],
                }
            ],
            36000,
        )
    ]


def test_parse_resource_descriptions_4():
    str_res_req = ["{gpu='YES'}/nodes=ALL+{gpu='NO'}/core=20"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [
        (
            [
                {
                    "property": "gpu='YES'",
                    "resources": [{"resource": "resource_id", "value": -1}],
                },
                {
                    "property": "gpu='NO'",
                    "resources": [{"resource": "core", "value": "20"}],
                },
            ],
            None,
        )
    ]


def test_parse_resource_descriptions_5():
    str_res_req = ["{gpu='YES'}/nodes=ALL", "{gpu='NO'}/core=20"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [
        (
            [
                {
                    "property": "gpu='YES'",
                    "resources": [{"resource": "resource_id", "value": -1}],
                }
            ],
            None,
        ),
        (
            [
                {
                    "property": "gpu='NO'",
                    "resources": [{"resource": "core", "value": "20"}],
                }
            ],
            None,
        ),
    ]


def test_parse_resource_descriptions_walltime_only():
    str_res_req = ["walltime=4:0"]
    res_req = parse_resource_descriptions(str_res_req, default_res, nodes_res)
    assert res_req == [
        (
            [
                {
                    "property": "",
                    "resources": [{"resource": "resource_id", "value": "1"}],
                }
            ],
            14400,
        )
    ]
