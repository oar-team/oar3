"""
    lib.plugins
    ~~~~~~~~~~~~~~~

    OAR plugin interface.
"""

import sys

# selectable entry points were introduced in Python 3.10
if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


def find_plugin_for_entry_point(entry_point_name):
    """Yield the tuples (name, class) of registered extra functions for a given entry point."""
    for entry_point in entry_points(group=entry_point_name):
        yield entry_point.name, entry_point.load()
