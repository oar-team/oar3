# coding: utf-8
import pytest

import os

from oar.lib import (db, AdmissionRule)
from oar.lib.submission import (JobParameters, parse_resource_descriptions, add_micheline_jobs,
                                default_submission_config)


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
