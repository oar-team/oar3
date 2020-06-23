import pytest

from oar.lib import config
from oar.lib.job_handling import insert_job

from tempfile import mkdtemp
import shutil

from flask import url_for


@pytest.fixture(scope='module', autouse=True)
def oar_conf(request):
    config['OAR_PROXY_INTERNAL'] = 'yes'

    @request.addfinalizer
    def remove_fairsharing():
        config['OAR_PROXY_INTERNAL'] = 'no'


def test_proxy_no_auth(client):
    res = client.get(url_for('proxy.proxy', job_id=11111111111111111))

    assert res.status_code == 403


def test_proxy_no_jobid(client):
    res = client.get(url_for('proxy.proxy', job_id=11111111111111111),
                     headers={'X_REMOTE_IDENT': 'bob'})

    assert res.status_code == 404


def test_proxy_no_proxy_file(client):
    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='bob')
    res = client.get(url_for('proxy.proxy', job_id=job_id),
                     headers={'X_REMOTE_IDENT': 'bob'})

    assert res.status_code == 404


def test_proxy(client):
    proxy_file_dir = mkdtemp()

    job_id = insert_job(res=[(60, [('resource_id=4', "")])], properties="", user='bob',
                        launching_directory=proxy_file_dir)

    with open('{}/OAR.{}.proxy.json'.format(proxy_file_dir, job_id), 'w', encoding="utf-8") as proxy_file_fd:
        proxy_file_fd.write('{{"url": "http://node1.acme.org:8899/oarapi-priv/proxy/{}"}}'.format(job_id))

    res = client.get(url_for('proxy.proxy', job_id=job_id),
                     headers={'X_REMOTE_IDENT': 'bob'})

    print(res)
    # import pdb; pdb.set_trace()
    assert res.status_code == 500
    shutil.rmtree(proxy_file_dir)
