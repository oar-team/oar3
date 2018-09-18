# coding: utf-8
import os
import re
import pytest

from sqlalchemy import text

from oar.lib import (db, AdmissionRule)
from oar.lib.submission import (JobParameters, default_submission_config)

@pytest.fixture(scope='function', autouse=True)
def builtin_config(request):
    default_submission_config()
                                
def default_job_parameters(**kwargs):
    default_job_params = {
        'job_type': 'PASSIVE',
        'resource': None,
        'name': 'yop',
        'project': 'yop',
        'command': 'sleep',
        'info_type': '',
        'queue': None,
        'properties': '',
        'checkpoint': 0,
        'signal': 12,
        'notify': '',
        'types': None,
        'directory': '/tmp',
        'dependencies': None,
        'stdout': None,
        'stderr': None,
        'hold': None,
        'initial_request': 'foo',
        'user': 'alice',
        'array_id': 0,
        'start_time': 0,
        'reservation_field': None
    }

    if kwargs:
        for key, value in kwargs.items():
            default_job_params[key] = value

    return JobParameters(**default_job_params)

def apply_admission_rules(job_parameters):

    # Read admission_rules
    rules_dir = os.path.dirname(__file__) +'/etc/oar/admission_rules.d/'
    file_names = os.listdir(rules_dir)
        
    file_names.sort()
    rules = ''
    for file_name in file_names:
        if re.match(r'^\d+_.*', file_name):
            with open(rules_dir + file_name, 'r') as rule_file:
                for line in rule_file:
                    rules += line

    # Apply rules
    code = compile(rules, '<string>', 'exec')

    exec(code, job_parameters.__dict__)

def test_00_default_queue():
    job_parameters = default_job_parameters()
    apply_admission_rules(job_parameters)
    assert job_parameters.queue == 'default'

def test_01_prevent_root_oar_toSubmit_ok():
    job_parameters = default_job_parameters(user='alice')
    apply_admission_rules(job_parameters)

def test_02_prevent_root_oar_toSubmit_bad():
    job_parameters = default_job_parameters(user='oar')
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)

def test_04_filter_bad_resources():
    # job_parameters.resource_request
    # [([{'property': '', 'resources': [{'resource': 'switch', 'value': '2'}, {'resource': 'resource_id', 'value': '10'}]}, {'property': "lic_type = 'mathlab'", 'resources': [{'resource': 'state', 'value': '2'}]}], 216000)]

    job_parameters = default_job_parameters(resource=["/switch=2/nodes=10+{lic_type = 'mathlab'}/state=2, walltime = 60"])
    with pytest.raises(Exception):
        apply_admission_rules(job_parameters)

def test_30_avoid_jobs_on_resources_in_drain_mode():
    job_parameters = default_job_parameters()
    apply_admission_rules(job_parameters)
    assert job_parameters.properties_applied_after_validation == "drain='NO'"

