# coding: utf-8
import pytest

import os

from oar.lib import (db, AdmissionRule)
from oar.lib.submission import (JobParameters, parse_resource_descriptions, add_micheline_jobs,
                                default_submission_config, scan_script)

import oar.lib.tools  # for monkeypatching

fake_popen_process_stdout = ''
class FakeProcessStdout(object):
    def __init__(self):
        pass
    def decode(self):
        return fake_popen_process_stdout
 
class FakePopen(object):
    def __init__(self, cmd, stdout):
        pass
    def communicate(self):
        process_sdtout = FakeProcessStdout() 
        return [process_sdtout]
    
@pytest.fixture(scope='function')
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(oar.lib.tools, 'Popen', FakePopen)

@pytest.fixture(scope='function', autouse=True)
def builtin_config(request):
    default_submission_config()

@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    db.delete_all()
    with db.session(ephemeral=True):
        db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost" + str(int(i / 2)))

        db.session.execute(AdmissionRule.__table__.delete())
        db['AdmissionRule'].create(rule="name='yop'")
        yield


def default_job_parameters(resource_request):
    return  JobParameters(
        job_type='PASSIVE',
        resource=resource_request,
        name='yop',
        project='yop',
        command='sleep',
        info_type='',
        queue='default',
        properties='',
        checkpoint=0,
        signal=12,
        notify='',
        types=None,
        directory='/tmp',
        dependencies=None,
        stdout=None,
        stderr=None,
        hold=None,
        initial_request='foo',
        user=os.environ['USER'],
        array_id=0,
        start_time=0,
        reservation_field=None
    )

def test_add_micheline_jobs_1():

    job_parameters = default_job_parameters(None)
    import_job_key_inline = ''
    import_job_key_file = ''
    export_job_key_file = ''
    (error, job_id_lst) = add_micheline_jobs(job_parameters, import_job_key_inline,\
                                           import_job_key_file, export_job_key_file)

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, '')
    assert len(job_id_lst) == 1


def test_add_micheline_jobs_2():

    job_parameters = default_job_parameters(None)
    import_job_key_inline = ''
    import_job_key_file = ''
    export_job_key_file = ''
    job_parameters.stdout = 'yop'
    job_parameters.stderr = 'poy'
    job_parameters.types = ['foo']

    (error, job_id_lst) = add_micheline_jobs(job_parameters, import_job_key_inline, \
                                           import_job_key_file, export_job_key_file)

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, '')
    assert len(job_id_lst) == 1

def test_add_micheline_simple_array_job():

    additional_config = {
        'OARSUB_DEFAULT_RESOURCES': 'network_address=2/resource_id=1+/resource_id=2',
        'OARSUB_NODES_RESOURCES': 'resource_id'
    }
    default_submission_config(additional_config)

    job_parameters = default_job_parameters(None)
    import_job_key_inline = ''
    import_job_key_file = ''
    export_job_key_file = ''
    job_parameters.types = ['foo']

    job_parameters.array_nb = 5
    job_parameters.array_params = [str(i) for i in range(job_parameters.array_nb)]

    # print(job_vars)

    (error, job_id_lst) = add_micheline_jobs(job_parameters, import_job_key_inline, \
                                           import_job_key_file, export_job_key_file)

    res = db['JobResourceGroup'].query.all()
    for item in res:
        print(item.to_dict())
    res = db['JobResourceDescription'].query.all()
    for item in res:
        print(item.to_dict())

    print("job id:", job_id_lst)
    print("error:", error)
    assert error == (0, '')
    assert len(job_id_lst) == 5

def test_scan_script(monkeypatch_tools):
    global fake_popen_process_stdout
    fake_popen_process_stdout = ("#Funky job\n"
                                 "#OAR -l nodes=10,walltime=3600\n"
                                 "#OAR -l gpu=10\n"
                                 "#OAR -q yop\n"
                                 "#OAR -p pa=b\n"
                                 "#OAR --checkpoint 12\n"
                                 "#OAR --notify noti-exec\n"
                                 "#OAR -d /tmp/\n"
                                 "#OAR -n funky\n"
                                 "#OAR --project batcave\n"
                                 "#OAR --hold\n"
                                 "#OAR -a 12\n"
                                 "#OAR -a 32\n"
                                 "#OAR --signal 12\n"
                                 "#OAR -O sto\n"
                                 "#OAR -E ste\n"
                                 "#OAR -k\n"
                                 "#OAR --import-job-key-inline-priv key\n"
                                 "#OAR -i key_file\n"
                                 "#OAR -e key_file\n"
                                 "#OAR -s stage_filein\n"
                                 "#OAR --stagein-md5sum file_md5sum\n"
                                 "#OAR --array 10\n"
                                 "#OAR --array-param-file p_file\n"
                                 "beast_application")

    result = {'initial_request': 'command -l nodes=10,walltime=3600 -l gpu=10 -q yop -p pa=b --checkpoint 12 --notify noti-exec -d /tmp/ -n funky --project batcave --hold -a 12 -a 32 --signal 12 -O sto -E ste -k --import-job-key-inline-priv key -i key_file -e key_file -s stage_filein --stagein -md5sum file_md5sum --array 10 --array-param-file p_file',
              'resource': ['nodes=10,walltime=3600', 'gpu=10'], 'queue': 'yop',
              'property': 'pa=b', 'checkpoint': 12, 'notify': 'noti-exec',
              'directory': '/tmp/', 'name': 'funky', 'project': 'batcave',
              'hold': True, 'dependencies': [12, 32], 'signal': 12, 'stdout': 'sto',
              'stderr': 'ste', 'use_job_key': True, 'import_job_key_inline': 'key',
              'import_job_key_file': 'key_file', 'export_job_key_file': 'key_file',
              'stagein': '-md5sum file_md5sum', 'array': 10, 'array_param_file': 'p_file'}

    
    (error, res) = scan_script('yop', 'command', 'zorglub')
    print(error, fake_popen_process_stdout, result)
    assert error == (0, '')
    assert res == result
