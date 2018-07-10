# -*- coding: utf-8 -*-
import pytest
import os

from io import BytesIO
from tempfile import mkstemp
from flask import url_for
import oar.lib.tools  # for monkeypatching

fake_popen_data = None

class FakePopen(object):
    def __init__(self, cmd, stdin):
        pass
    def communicate(self,data):
        global fake_popen_data
        fake_popen_data = data
    def kill(self):
        pass

fake_call_retcodes = []
fake_calls = []
def fake_call(x):
    fake_calls.append(x)
    print('fake_call: ', x)
    return fake_call_retcodes.pop(0)

fake_check_outputs = []
fake_check_output_cmd = []
def fake_check_output(cmd):
    fake_check_output_cmd.append(cmd)
    return fake_check_outputs.pop(0)

def fake_getpwnam(user):
    class FakePw():
        def __init__(self, user):
            self.pw_dir = '/home/' + user
    return FakePw(user)

@pytest.fixture(scope="module", autouse=True)
def set_env(request):
    os.environ['OARDIR'] = '/tmp'
    
@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'Popen', FakePopen)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call)
    monkeypatch.setattr(oar.lib.tools, 'check_output', fake_check_output)
    monkeypatch.setattr(oar.lib.tools, 'getpwnam', fake_getpwnam)

def test_app_media_ls_file_forbidden(client):
    assert client.get(url_for('media.ls', path='yop')).status_code == 403
    
def test_app_media_ls_file(client):
    global fake_calls
    fake_calls = []
    global fake_call_retcodes
    fake_call_retcodes = [0,0]
    
    global fake_check_outputs
    fake_check_outputs = [b'yop\nzozo\n',
                          b'43ff_53248_1530972662_directory\n81a4_2509_1530883655_regular file\n']
    res = client.get(url_for('media.ls', path='yop'), headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 200
    print(res.json, len(res.json['items']))
    print(fake_calls)
    print(fake_check_output_cmd)
    assert len(res.json['items']) == 2

def test_app_media_get_file_not_exit(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]
    res = client.get(url_for('media.get_file', path_filename='yop'),
                      headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 404

def test_app_media_get_file_unreadble(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 1]
    res = client.get(url_for('media.get_file', path_filename='yop'),
                      headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 403

def test_app_media_get_file(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]
    global fake_check_outputs
    fake_check_outputs = [b'fake content']
    res = client.get(url_for('media.get_file', path_filename='yop'),
                      headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 200
    print(res.data)
    assert res.data == b'fake content'

def test_app_media_get_file_tail(client):
    global fake_call_retcodes
    fake_call_retcodes = [0, 0]
    global fake_check_outputs
    fake_check_outputs = [b'fake content']
    res = client.get(url_for('media.get_file', path_filename='yop', tail=1),
                      headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 200
    print(res.data)
    assert res.data == b'fake content'
    
def test_app_media_post_file_already_exist(client):
    global fake_call_retcodes
    fake_call_retcodes = [0]
    temp_path = '~/tmp/yop'
    res = client.post(url_for('media.post_file', path_filename=temp_path),
                      data = {'file': (BytesIO(b'my file contents'), 'toto.txt')},
                      headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 403    

def test_app_media_post_file(client):
    global fake_call_retcodes
    fake_call_retcodes = [1]
    _, temp_path = mkstemp()
    res = client.post(url_for('media.post_file', path_filename='~' + temp_path),
                      data = {'file': (BytesIO(b'my file contents'), 'toto.txt')},
                      headers={'X_REMOTE_IDENT': 'bob'})
    assert res.status_code == 200

    
def test_app_media_delete_file(client):
    pass

def test_app_media_chmod(client):
    pass
