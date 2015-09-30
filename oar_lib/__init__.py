# -*- coding: utf-8 -*-
"""
    oar-lib
    ~~~~~~~

    Python version of OAR Common Library

"""
from types import ModuleType
import sys

from .compat import iteritems

__version__ = '0.4.1'

# The implementation of a lazy-loading module in this file replaces the
# oar package when imported from within.  Attribute access to the oar
# module will then lazily import from the modules that implement the objects.

# import mapping to objects in other modules
all_by_module = {
    'oar_lib.models': [
        'db', 'Accounting', 'AdmissionRule', 'AssignedResource', 'Challenge',
        'EventLog', 'EventLogHostname', 'File', 'FragJob',
        'GanttJobsPrediction', 'GanttJobsPredictionsLog',
        'GanttJobsPredictionsVisu', 'GanttJobsResource',
        'GanttJobsResourcesLog', 'GanttJobsResourcesVisu', 'Job',
        'JobDependencie', 'JobResourceDescription', 'JobResourceGroup',
        'JobStateLog', 'JobType', 'MoldableJobDescription', 'Queue',
        'Resource', 'ResourceLog', 'Scheduler',
    ],
    'oar_lib.exceptions': [
        'OARException', 'InvalidConfiguration', 'DatabaseError',
        'DoesNotExist',
    ],
    'oar_lib.database': ['Database'],
    'oar_lib.logging': ['create_logger', 'get_logger'],
    'oar_lib.configuration': ['Configuration'],
    'oar_lib.utils': ['cached_property', 'Command', 'JSONEncoder',
                      'render_query', 'ResultProxyIter', 'row2dict'],
    'oar_lib.globals': ['config', 'logger'],
    'oar_lib.fixture': ['load_fixtures', 'dump_fixtures'],
}

# modules that should be imported when accessed as attributes of oar
attribute_modules = frozenset(['configuration', 'database', 'exceptions',
                               'logging', 'models', 'utils', 'fixture',
                               'psycopg2', 'compat', 'basequery'])


object_origins = {}
for module, items in iteritems(all_by_module):
    for item in items:
        object_origins[item] = module


class Module(ModuleType):
    """Automatically import objects from the modules."""

    def __getattr__(self, name):
        if name in object_origins:
            module = __import__(object_origins[name], None, None, [name])
            for extra_name in all_by_module[module.__name__]:
                setattr(self, extra_name, getattr(module, extra_name))
            return getattr(module, name)
        elif name in attribute_modules:
            __import__('oar_lib.' + name)
        return ModuleType.__getattribute__(self, name)

    def __dir__(self):
        """Just show what we want to show."""
        result = list(new_module.__all__)
        result.extend(('__file__', '__path__', '__doc__', '__all__',
                       '__docformat__', '__name__', '__path__',
                       '__package__', '__version__'))
        return result

# keep a reference to this module so that it's not garbage collected
old_module = sys.modules['oar_lib']


# setup the new module and patch it into the dict of loaded modules
new_module = sys.modules['oar_lib'] = Module('oar_lib')
new_module.__dict__.update({
    '__file__': __file__,
    '__package__': 'oar_lib',
    '__path__': __path__,  # noqa
    '__doc__': __doc__,
    '__version__': __version__,
    '__all__': tuple(object_origins) + tuple(attribute_modules),
    '__docformat__': 'restructuredtext en',
    'VERSION': __version__,
})
