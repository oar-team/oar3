# -*- coding: utf-8 -*-
import oar.lib

from . import assert_raises


def test_all_module_api():

    all_modules = [
        'Accounting', 'AdmissionRule', 'AssignedResource',
        'Challenge', 'Command', 'Configuration', 'Database', 'DatabaseError',
        'DoesNotExist', 'EventLog', 'EventLogHostname', 'File', 'FragJob',
        'GanttJobsPrediction', 'GanttJobsPredictionsLog',
        'GanttJobsPredictionsVisu', 'GanttJobsResource',
        'GanttJobsResourcesLog', 'GanttJobsResourcesVisu',
        'InvalidConfiguration', 'JSONEncoder', 'Job', 'JobDependencie',
        'JobResourceDescription', 'JobResourceGroup', 'JobStateLog', 'JobType',
        'MoldableJobDescription', 'OARException', 'Queue', 'Resource',
        'ResourceLog', 'ResultProxyIter', 'Scheduler', '__all__', '__doc__',
        '__docformat__', '__file__', '__name__', '__package__', '__path__',
        '__path__', '__version__', 'basequery', 'cached_property', 'compat',
        'config', 'configuration', 'create_logger', 'database', 'db',
        'dump_fixtures', 'exceptions', 'fixture', 'get_logger',
        'load_fixtures', 'logger', 'logging', 'models', 'psycopg2',
        'render_query', 'row2dict', 'utils']

    assert set(all_modules) == set(dir(oar.lib))
    with assert_raises(ImportError):
        from oar.lib import totototo  # noqa
