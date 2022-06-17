import pytest

import oar.lib.tools  # for monkeypatching
from oar.lib import config
from oar.lib.job_handling import insert_job

fake_call_retcodes = []
fake_calls = []


def fake_call(x, env=None):
    fake_calls.append(x)
    print("fake_call: ", x)
    return fake_call_retcodes.pop(0)


fake_check_outputs = []
fake_check_output_cmd = []


def fake_check_output(cmd, env=None):
    fake_check_output_cmd.append(cmd)
    return fake_check_outputs.pop(0)


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch, monkeypatch_scoped_session):
    monkeypatch.setattr(oar.lib.tools, "call", fake_call)
    monkeypatch.setattr(oar.lib.tools, "check_output", fake_check_output)


@pytest.fixture(scope="module", autouse=True)
def oar_conf(request):
    config["PROXY"] = "traefik"

    @request.addfinalizer
    def remove_fairsharing():
        config["PROXY"] = "no"


def test_proxy_no_auth(client):
    res = client.get("/proxy/{job_id}".format(job_id=11111111111111111))
    print(res.json())
    assert res.status_code == 403


def test_proxy_no_jobid(client):
    res = client.get(
        "/proxy/{job_id}".format(job_id=11111111111111111),
        headers={"x-remote-ident": "bob"},
    )

    assert res.status_code == 404


def test_proxy_no_proxy_file(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]

    job_id = insert_job(res=[(60, [("resource_id=4", "")])], properties="", user="bob")
    res = client.get(
        "/proxy/{job_id}".format(job_id=job_id), headers={"x-remote-ident": "bob"}
    )

    assert res.status_code == 404


def test_proxy(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]

    job_id = insert_job(
        res=[(60, [("resource_id=4", "")])],
        properties="",
        user="bob",
        launching_directory="/tmp",
    )

    global fake_check_outputs

    fake_check_outputs = [
        '{{"url": "http://node1.acme.org:8899/oarapi-priv/proxy/{}"}}'.format(job_id)
    ]

    res = client.get(
        "/proxy/{job_id}".format(job_id=job_id), headers={"x-remote-ident": "bob"}
    )

    print(res)
    # import pdb; pdb.set_trace()
    assert res.status_code == 500
