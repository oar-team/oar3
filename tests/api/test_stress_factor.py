# -*- coding: utf-8 -*-
import os

import pytest

import oar.lib.tools  # for monkeypatching

fake_popen_data = None


class FakePopen(object):
    def __init__(self, cmd, stdin):
        pass

    def communicate(self, data):
        global fake_popen_data
        fake_popen_data = data

    def kill(self):
        pass


fake_call_retcodes = []
fake_calls = []


def fake_call(x):
    fake_calls.append(x)
    print("fake_call: ", x)
    return fake_call_retcodes.pop(0)


fake_check_outputs = []
fake_check_output_cmd = []


def fake_check_output(cmd):
    fake_check_output_cmd.append(cmd)
    return fake_check_outputs.pop(0)


@pytest.fixture(scope="module", autouse=True)
def set_env(request, backup_and_restore_environ_module):
    os.environ["OARDIR"] = "/tmp"


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "check_output", fake_check_output)


def test_stress_factor_success(client):
    global fake_check_outputs
    fake_check_outputs = [b"GLOBAL_STRESS=0.15\n"]
    res = client.get("/stress_factor")

    assert res.status_code == 200
    print(res.json())
    print(fake_check_output_cmd)
    assert res.json()["GLOBAL_STRESS"] == "0.15"


def test_stress_factor_failed(client):
    global fake_check_outputs
    fake_check_outputs = [b"bad\n"]
    res = client.get("/stress_factor")

    print(res.json())
    print(fake_check_output_cmd)
    assert res.status_code == 404
