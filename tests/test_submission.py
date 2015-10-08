# coding: utf-8
from __future__ import unicode_literals, print_function
import pytest

import os

from oar.lib import (db, AdmissionRule)
from oar.lib.submission import parse_resource_descriptions, add_micheline_jobs


@pytest.yield_fixture(scope='function', autouse=True)
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        if not db['Queue'].query.all():
            db['Queue'].create(name='default', priority=3, scheduler_policy='kamelot', state='Active')

        # add some resources
        for i in range(5):
            db['Resource'].create(network_address="localhost" + str(int(i / 2)))
        yield


def test_add_micheline_jobs():

    con = db.session(reflect=False).bind.connect()
    con.execute(AdmissionRule.__table__.delete())

    db['AdmissionRule'].create(rule="name='yop'")

    default_resources = '/resource_id=1'
    resource_request = parse_resource_descriptions(None, default_resources, 'resource_id')
    job_vars = {
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
