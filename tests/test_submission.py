# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

import os

from oar.lib import (db, AdmissionRule)
from oar.lib.submission import (parse_resource_descriptions, add_micheline_jobs, set_not_cli)


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        if not db['Queue'].query.all():
            db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost" + str(int(i / 2)))

        con = db.session(reflect=False).bind.connect()
        con.execute(AdmissionRule.__table__.delete())
        db['AdmissionRule'].create(rule="name='yop'")

        yield


@pytest.fixture(scope='function', autouse=True)
def not_cli():
    set_not_cli()


def default_job_vars(resource_request):
    return {
        'job_type': 'PASSIVE',
        'resource_request': resource_request,
        'name': 'yop',
        'project': 'yop',
        'command': 'sleep',
        'info_type': '',
        'queue_name': 'default',
        'properties': '',
        'checkpoint': 0,
        'signal': 12,
        'notify': '',
        'types': None,
        'launching_directory': '/tmp',
        'dependencies': None,
        'stdout': None,
        'stderr': None,
        'hold': None,
        'initial_request': 'foo',
        'user': os.environ['USERNAME'],
        'array_id': 0,
        'start_time': '0',
        'reservation_field': None
    }


def test_add_micheline_jobs_1():

    default_resources = '/resource_id=1'
    resource_request = parse_resource_descriptions(None, default_resources, 'resource_id')
    job_vars = default_job_vars(resource_request)

    reservation_date = ''
    use_job_key = False
    import_job_key_inline = ''
    import_job_key_file = ''
    export_job_key_file = ''
    initial_request = 'yop'
    array_nb = 0
    array_params = []

    # print(job_vars)

    (err, job_id_lst) = add_micheline_jobs(job_vars, reservation_date, use_job_key,
                                           import_job_key_inline, import_job_key_file,
                                           export_job_key_file,
                                           initial_request, array_nb, array_params)

    print("job id:", job_id_lst)
    assert len(job_id_lst) == 1


def test_add_micheline_jobs_2():

    default_resources = '/resource_id=1'
    resource_request = parse_resource_descriptions(None, default_resources, 'resource_id')
    job_vars = default_job_vars(resource_request)
    job_vars['stdout'] = 'yop'
    job_vars['stderr'] = 'poy'
    job_vars['types'] = 'foo'

    reservation_date = ''
    use_job_key = False
    import_job_key_inline = ''
    import_job_key_file = ''
    export_job_key_file = ''
    initial_request = 'yop'
    array_nb = 0
    array_params = []

    # print(job_vars)

    (err, job_id_lst) = add_micheline_jobs(job_vars, reservation_date, use_job_key,
                                           import_job_key_inline, import_job_key_file,
                                           export_job_key_file,
                                           initial_request, array_nb, array_params)

    print("job id:", job_id_lst)
    assert len(job_id_lst) == 1


def test_add_micheline_simple_array_job():

    default_resources = 'network_address=2/resource_id=1+/resource_id=2'
    resource_request = parse_resource_descriptions(None, default_resources, 'resource_id')
    job_vars = default_job_vars(resource_request)
    job_vars['types'] = 'foo'

    reservation_date = ''
    use_job_key = False
    import_job_key_inline = ''
    import_job_key_file = ''
    export_job_key_file = ''
    initial_request = 'yop'
    array_nb = 5
    array_params = [str(i) for i in range(array_nb)]

    # print(job_vars)

    (err, job_id_lst) = add_micheline_jobs(job_vars, reservation_date, use_job_key,
                                           import_job_key_inline, import_job_key_file,
                                           export_job_key_file,
                                           initial_request, array_nb, array_params)

    r = db['JobResourceGroup'].query.all()
    for item in r:
        print(item.to_dict())
    r = db['JobResourceDescription'].query.all()
    for item in r:
        print(item.to_dict())

    print("job id:", job_id_lst)
    assert len(job_id_lst) == 5
