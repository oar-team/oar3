# -*- coding: utf-8 -*-
"""
    oar-lib
    ~~~~~~~

    Python version of OAR Common Library

"""
import sys
from types import ModuleType

# The implementation of a lazy-loading module in this file replaces the
# oar package when imported from within.  Attribute access to the oar
# module will then lazily import from the modules that implement the objects.

# import mapping to objects in other modules
all_by_module = {
    "oar.lib.models": [
        "db",
        "Accounting",
        "AdmissionRule",
        "AssignedResource",
        "Challenge",
        "EventLog",
        "EventLogHostname",
        "File",
        "FragJob",
        "GanttJobsPrediction",
        "GanttJobsPredictionsLog",
        "GanttJobsPredictionsVisu",
        "GanttJobsResource",
        "GanttJobsResourcesLog",
        "GanttJobsResourcesVisu",
        "Job",
        "JobDependencie",
        "JobResourceDescription",
        "JobResourceGroup",
        "JobStateLog",
        "JobType",
        "MoldableJobDescription",
        "Queue",
        "Resource",
        "ResourceLog",
        "Scheduler",
        "WalltimeChange",
    ],
    "oar.lib.exceptions": [
        "OARException",
        "InvalidConfiguration",
        "DatabaseError",
        "DoesNotExist",
    ],
    "oar.lib.database": ["Database"],
    "oar.lib.logging": ["create_logger", "get_logger", "logger"],
    "oar.lib.configuration": ["Configuration"],
    "oar.lib.utils": [
        "cached_property",
        "Command",
        "JSONEncoder",
        "render_query",
        "ResultProxyIter",
        "row2dict",
    ],
    "oar.lib.plugins": ["find_plugin_for_entry_point"],
    "oar.lib.globals": [""],
    "oar.lib.fixture": ["load_fixtures", "dump_fixtures"],
}

# modules that should be imported when accessed as attributes of oar
attribute_modules = frozenset(
    [
        "configuration",
        "database",
        "tools",
        "exceptions",
        "logging",
        "models",
        "utils",
        "fixture",
        "psycopg2",
        "basequery",
    ]
)

# Reverse the all_by_module to have the name -> location direct access.
object_origins = {}
for module, items in all_by_module.items():
    for item in items:
        object_origins[item] = module


class Module(ModuleType):
    """Automatically import objects from the modules."""

    # __getattr__ is only called when an attribute access fails with an AttributeError.
    # This causes this function to be called only when the imported name is not found.
    def __getattr__(self, name):
        if name in object_origins:
            module = __import__(object_origins[name], None, None, [name])
            for extra_name in all_by_module[module.__name__]:
                # Expose in self, all the attributes defined by the module that is currently loaded.
                setattr(self, extra_name, getattr(module, extra_name))
            return getattr(module, name)
        elif name in attribute_modules:
            # If name refers to an exposed module,
            # load it so it can be accessed normally (using import utils from oar.lib.<name>)
            __import__("oar.lib." + name)
        return ModuleType.__getattribute__(self, name)

    def __dir__(self):
        """Just show what we want to show."""
        result = list(new_module.__all__)
        result.extend(
            (
                "__file__",
                "__path__",
                "__doc__",
                "__all__",
                "__docformat__",
                "__name__",
                "__path__",
                "__package__",
                "__version__",
            )
        )
        return result


# keep a reference to this module so that it's not garbage collected
old_module = sys.modules["oar.lib"]


# setup the new module and patch it into the dict of loaded modules
new_module = sys.modules["oar.lib"] = Module("oar.lib")
new_module.__dict__.update(
    {
        "__file__": __file__,
        "__package__": "oar.lib",
        "__path__": __path__,  # noqa
        "__doc__": __doc__,
        "__all__": tuple(object_origins) + tuple(attribute_modules),
        "__docformat__": "restructuredtext en",
    }
)
