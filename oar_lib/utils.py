# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import, unicode_literals

import threading
import subprocess
import decimal
import datetime

from inspect import isgenerator
from collections import OrderedDict

from .compat import numeric_types, to_unicode, json


class JSONEncoder(json.JSONEncoder):
    """JSON Encoder class that handles conversion for a number of types not
    supported by the default json library, especially the sqlalchemy objects.

    :returns: object that can be converted to json
    """

    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        elif isinstance(obj, (decimal.Decimal)):
            return to_unicode(obj)
        elif hasattr(obj, 'asdict') and callable(getattr(obj, 'asdict')):
            return obj.asdict()
        elif hasattr(obj, 'to_dict') and callable(getattr(obj, 'to_dict')):
            return obj.to_dict()
        else:
            return json.JSONEncoder.default(self, obj)


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


class ResultProxyIter(list):
    """ SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them. """

    def __init__(self, result_proxies):
        self.count = result_proxies.rowcount
        if not isgenerator(result_proxies):
            result_proxies = iter((result_proxies, ))
        self.result_proxies = result_proxies
        self._iter = None

    def __next__(self):
        if self._iter is None:
            rp = next(self.result_proxies)
            self.keys = list(rp.keys())
            self._iter = iter(rp.fetchall())
        try:
            return row2dict(next(self._iter))
        except StopIteration:
            self._iter = None
            return self.__next__()

    def __len__(self):
        return self.count

    next = __next__

    def __iter__(self):
        return self


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


def try_convert_decimal(value):
    """Try to convert ``value`` to a decimal."""
    if value.isdecimal():
        for _type in numeric_types:
            try:
                return _type(value)
            except:
                pass
    return value


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
