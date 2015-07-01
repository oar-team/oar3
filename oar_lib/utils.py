# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import, unicode_literals

import threading
import subprocess

from collections import OrderedDict

from .compat import numeric_types


def try_convert_decimal(value):
    """Try to convert ``value`` to a decimal."""
    if value.isdecimal():
        for _type in numeric_types:
            try:
                return _type(value)
            except:
                pass
    return value


class SimpleNamespace(dict):
    def __init__(self, *args, **kwargs):
        super(SimpleNamespace, self).__init__(*args, **kwargs)
        self.__dict__ = self


class CachedProperty(object):
    """A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property."""
    def __init__(self, func):
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__.setdefault("_cache", {})
        cached_value = obj._cache.get(self.__name__, None)
        if cached_value is None:
            # don't cache None value
            obj._cache[self.__name__] = self.func(obj)
        return obj._cache.get(self.__name__)


cached_property = CachedProperty


def row2dict(row, ignore_keys=()):
    """Converts sqlalchemy RowProxy to an OrderedDict."""
    result = OrderedDict()
    for key in row.keys():
        if key not in ignore_keys:
            result[key] = getattr(row, key)
    return result


def render_query(statement, bind=None, reindent=True):
    """Generate an SQL expression string with bound parameters rendered inline
    for the given SQLAlchemy statement.

    The function can also receive a `sqlalchemy.orm.Query` object instead of
    statement.
    """
    from sqlalchemy_utils.functions import render_statement
    raw_sql = render_statement(statement, bind)
    try:
        import sqlparse
        return sqlparse.format(raw_sql, reindent=reindent)
    except ImportError:
        return raw_sql


class Command(object):
    """
    Run subprocess commands in a different thread with TIMEOUT option.
    Based on jcollado's solution:
    http://stackoverflow.com/questions/1191374/subprocess-with-timeout/4825933#4825933
    """
    def __init__(self, cmd):
        from . import logger
        self.cmd = cmd
        self.process = None
        self.logger = logger

    def run(self, timeout):
        def target():
            self.process = subprocess.Popen(self.cmd, shell=True)
            self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()

        error = None
        thread.join(timeout)
        if thread.is_alive():
            self.logger.error('Timeout: Terminating process "%s"' % self.cmd)
            self.process.terminate()
            thread.join()

        return (error, self.process.returncode)

    def __call__(self, *args, **kwargs):
        self.run(*args, **kwargs)
