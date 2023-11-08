# -*- coding: utf-8 -*-
import os
from io import BytesIO
from tempfile import mkstemp

import pytest

import oar.lib.tools  # for monkeypatching

fake_popen_data = None


class FakePopen(object):
    def __init__(self, cmd, env, stdin):
        pass

    def communicate(self, data):
        global fake_popen_data
        fake_popen_data = data

    def kill(self):
        pass


fake_call_retcodes = []
fake_calls = []


def fake_call(x, env):
    fake_calls.append(x)
    # print("fake_call: ", x, env)
    return fake_call_retcodes.pop(0)


fake_check_outputs = []
fake_check_output_cmd = []


def fake_check_output(cmd, env):
    fake_check_output_cmd.append(cmd)
    return fake_check_outputs.pop(0)


def fake_getpwnam(user):
    class FakePw:
        def __init__(self, user):
            self.pw_dir = "/home/" + user

    return FakePw(user)


@pytest.fixture(scope="module", autouse=True)
def set_env(request, backup_and_restore_environ_module):
    os.environ["OARDIR"] = "/tmp"


@pytest.fixture(scope="function", autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, "Popen", FakePopen)
    monkeypatch.setattr(oar.lib.tools, "call", fake_call)
    monkeypatch.setattr(oar.lib.tools, "check_output", fake_check_output)
    monkeypatch.setattr(oar.lib.tools, "getpwnam", fake_getpwnam)


def test_app_media_ls_file_forbidden(client):
    assert client.get("/media/ls/{path}".format(path="yop")).status_code == 403


def test_app_media_ls_file(client):
    global fake_calls
    fake_calls = []
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]

    global fake_check_outputs
    fake_check_outputs = [
        b"yop\nzozo\n",
        b"43ff_53248_1530972662_directory\n81a4_2509_1530883655_regular file\n",
    ]
    res = client.get(
        "/media/ls/{path}".format(path="yop"),
        params={},
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 200
    print(len(res.json()["items"]))

    print(fake_calls)
    print(fake_check_output_cmd)
    assert len(res.json()["items"]) == 2


def test_app_media_get_file_not_exit(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]
    res = client.get(
        "/media/?path_filename={path_filename}".format(path_filename="yop"),
        headers={"x-remote-ident": "bob"},
    )
    print(res.json())
    assert res.status_code == 404


def test_app_media_get_file_unreadble(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 1]
    res = client.get(
        "/media/?path_filename={path_filename}".format(path_filename="yop"),
        headers={"x-remote-ident": "bob"},
    )
    print(res.__dict__)
    assert res.status_code == 403


def test_app_media_get_file(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]
    global fake_check_outputs
    fake_check_outputs = [b"fake content"]
    res = client.get(
        "/media/?path_filename={path_filename}".format(path_filename="yop"),
        headers={"x-remote-ident": "bob"},
    )
    assert res.status_code == 200
    print(res.__dict__)
    assert res._content == b"fake content"


def test_app_media_get_file_tail(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]
    global fake_check_outputs
    fake_check_outputs = [b"fake content"]
    res = client.get(
        "/media/?path_filename={path_filename}".format(path_filename="yop"),
        params={"tail": 1},
        headers={"x-remote-ident": "bob"},
    )
    assert res.status_code == 200
    print(res._content)
    assert res._content == b"fake content"


def test_app_media_post_file_already_exist(client):
    global fake_call_retcodes
    fake_call_retcodes = [0]
    temp_path = "~/tmp/yop"
    res = client.post(
        "/media/",
        files={"file": (temp_path, b"dummy content", "multipart/form-data")},
        headers={"x-remote-ident": "bob"},
    )
    print(res.__dict__)
    assert res.status_code == 403


def test_app_media_post_file(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]
    _, temp_path = mkstemp()

    res = client.post(
        "/media/",
        files={
            # (filename, filecontent, "type")
            "file": (temp_path, BytesIO(b"my file contents"), "multipart/form-data")
        },
        headers={"x-remote-ident": "bob"},
    )

    assert res.status_code == 200


def test_app_media_delete_file_not_exit(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]
    res = client.delete(
        "/media/?path_filename={path_filename}".format(path_filename="yop"),
        headers={"x-remote-ident": "bob"},
    )
    assert res.status_code == 404


def test_app_media_delete_file_unreadable(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 1]
    res = client.delete(
        "/media/?path_filename={}".format("yop"), headers={"x-remote-ident": "bob"}
    )
    print(res.__dict__)
    assert res.status_code == 403


def test_app_media_delete_file_rm_error(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0, 1]

    res = client.delete(
        "/media/?path_filename={}".format("yop"), headers={"x-remote-ident": "bob"}
    )
    assert res.status_code == 501


def test_app_media_delete_file(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0, 0]
    res = client.delete(
        "/media/?path_filename={path_filename}".format(path_filename="yop"),
        headers={"x-remote-ident": "bob"},
    )

    assert res.status_code == 204


def test_app_media_chmod_file_not_exit(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]
    res = client.post(
        "/media/chmod?path_filename={path_filename}&mode={mode}".format(
            path_filename="yop", mode="755"
        ),
        headers={"x-remote-ident": "bob"},
    )

    print(res.__dict__)
    assert res.status_code == 404


def test_app_media_chmod_file_not_alnum(client):
    global fake_call_retcodes
    fake_call_retcodes = [0]
    res = client.post(
        "/media/chmod?path_filename={path_filename}&mode={mode}".format(
            path_filename="yop", mode="###"
        ),
        headers={"x-remote-ident": "bob"},
    )
    assert res.status_code == 400


def test_app_media_chmod_file_chmod_error(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 1]
    res = client.post(
        "/media/chmod?path_filename={path_filename}&mode={mode}".format(
            path_filename="yop", mode="755"
        ),
        headers={"x-remote-ident": "bob"},
    )
    assert res.status_code == 500


def test_app_media_chmod_file_chmod(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]
    res = client.post(
        "/media/chmod?path_filename={path_filename}&mode={mode}".format(
            path_filename="yop", mode="755"
        ),
        headers={"x-remote-ident": "bob"},
    )
    assert res.status_code == 202
