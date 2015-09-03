# -*- coding: utf-8 -*-
from __future__ import with_statement
import pytest

from contextlib import contextmanager


DEFAULT_CONFIG = {
    'COSYSTEM_HOSTNAME': '127.0.0.1',
    'CPUSET_PATH': '/oar',
    'DB_BASE_FILE': ':memory:',
    'DB_TYPE': 'sqlite',
    'DEPLOY_HOSTNAME': '127.0.0.1',
    'DETACH_JOB_FROM_SERVER': 1,
    'ENERGY_SAVING_INTERNAL': 'no',
    'FINAUD_FREQUENCY': 300,
    'JOB_RESOURCE_MANAGER_FILE': '/etc/oar/job_resource_manager_cgroups.pl',
    'JOB_RESOURCE_MANAGER_PROPERTY_DB_FIELD': 'cpuset',
    'LOG_CATEGORIES': 'all',
    'LOG_FILE': '',
    'LOG_FORMAT': '[%(levelname)s] [%(asctime)s] [%(name)s]: %(message)s',
    'LOG_LEVEL': 3,
    'OAREXEC_DEBUG_MODE': 0,
    'OARSH_OARSTAT_CMD': '/usr/local/bin/oarstat',
    'OARSH_OPENSSH_DEFAULT_OPTIONS':
        '-oProxyCommand=none -oPermitLocalCommand=no',
    'OARSTAT_DEFAULT_OUTPUT_FORMAT': 2,
    'OARSUB_DEFAULT_RESOURCES': '/resource_id=1',
    'OARSUB_FORCE_JOB_KEY': 'no',
    'OARSUB_NODES_RESOURCES': 'network_address',
    'OAR_RUNTIME_DIRECTORY': '/var/lib/oar',
    'OPENSSH_CMD': '/usr/bin/ssh -p 6667',
    'PINGCHECKER_SENTINELLE_SCRIPT_COMMAND':
        '/usr/local/lib/oar/sentinelle.pl -t 30 -w 20',
    'PINGCHECKER_TAKTUK_ARG_COMMAND':
        'broadcast exec timeout 5 kill 9 [ true ]',
    'SCHEDULER_AVAILABLE_SUSPENDED_RESOURCE_TYPE': 'default',
    'SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER': 30,
    'SCHEDULER_GANTT_HOLE_MINIMUM_TIME': 300,
    'SCHEDULER_JOB_SECURITY_TIME': 60,
    'SCHEDULER_NB_PROCESSES': 1,
    'SCHEDULER_PRIORITY_HIERARCHY_ORDER': 'network_address/resource_id',
    'SCHEDULER_RESOURCE_ORDER': 'scheduler_priority ASC, state_num ASC, '
                                'available_upto DESC, suspended_jobs ASC, '
                                'network_address ASC, resource_id ASC',
    'SCHEDULER_TIMEOUT': 30,
    'SERVER_HOSTNAME': 'server',
    'SERVER_PORT': 6666,
    'SQLALCHEMY_CONVERT_UNICODE': True,
    'SQLALCHEMY_ECHO': False,
    'SQLALCHEMY_MAX_OVERFLOW': None,
    'SQLALCHEMY_POOL_RECYCLE': None,
    'SQLALCHEMY_POOL_SIZE': None,
    'SQLALCHEMY_POOL_TIMEOUT': None,
    'TAKTUK_CMD': '/usr/bin/taktuk -t 30 -s',
}


@contextmanager
def assert_raises(exception_class, msg=None):
    """Check that an exception is raised and its message contains `msg`."""
    with pytest.raises(exception_class) as exception:
        yield
    if msg is not None:
        message = '%s' % exception
        assert msg.lower() in message.lower()
