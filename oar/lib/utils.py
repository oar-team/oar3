# -*- coding: utf-8 -*-

import copy
import datetime
import decimal
import os
import re
import sys
from collections import OrderedDict
from collections.abc import Callable
from decimal import Decimal, InvalidOperation

import simplejson as json

basestring = (str, bytes)
integer_types = (int,)
numeric_types = integer_types + (float,)
builtin_str = str

try:  # pragma: no cover
    import __pypy__

    is_pypy = True
except Exception:  # pragma: no cover
    __pypy__ = None
    is_pypy = False


def dict_ps_copy(dict_ps):
    d = {}
    for key, value in dict_ps.items():
        if type(value) == dict:
            d[key] = {k: copy.copy(v) for k, v in value.items()}
        else:
            d[key] = value
    return d
    # return {k: ps_copy(v) for k,v in dict_ps.items()}


def with_metaclass(meta, base=object):
    return meta("NewBase", (base,), {})


def to_unicode(obj, encoding="utf-8"):
    """
    Convert ``obj`` to unicode"""
    # unicode support
    if isinstance(obj, str):
        return obj

    # bytes support
    if is_bytes(obj):
        if hasattr(obj, "tobytes"):
            return str(obj.tobytes(), encoding)
        return str(obj, encoding)

    # string support
    if isinstance(obj, basestring):
        if hasattr(obj, "decode"):
            return obj.decode(encoding)
        else:
            return str(obj, encoding)

    return str(obj)


def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


def is_bytes(x):
    return isinstance(x, (bytes, memoryview, bytearray))


def callable(obj):
    return isinstance(obj, Callable)


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
        elif hasattr(obj, "asdict") and callable(getattr(obj, "asdict")):
            return obj.asdict()
        elif hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
            return obj.to_dict()
        else:
            return json.JSONEncoder.default(self, obj)


def to_json(obj, **kwargs):
    """Dumps object to json string."""
    kwargs.setdefault("ensure_ascii", False)
    kwargs.setdefault("cls", JSONEncoder)
    kwargs.setdefault("indent", 4)
    kwargs.setdefault("separators", (",", ": "))
    return json.dumps(obj, **kwargs)


def touch(fname, times=None):
    dirname = "/".join(fname.split("/")[:-1])
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    with open(fname, "a"):
        os.utime(fname, times)


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

    def __delete__(self, obj):
        cache_obj = getattr(obj, "_cache", {})
        if self.__name__ in cache_obj:
            del cache_obj[self.__name__]


cached_property = CachedProperty


class ResultProxyIter(list):
    """SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them."""

    def __init__(self, result_proxy):
        self.items = result_proxy.fetchall()
        self._iter = iter(self.items)

    def __len__(self):
        return len(self.items)

    def __next__(self):
        try:
            return row2dict(next(self._iter))
        except StopIteration:
            self._iter = iter(self.items)
            exc_type, exc_value, tb = sys.exc_info()
            reraise(exc_type, exc_value, tb.tb_next)

    def __iter__(self):
        return self

    next = __next__


def try_convert_decimal(raw_value):
    """Try to convert ``value`` to a decimal."""
    value = to_unicode(raw_value)
    try:
        Decimal(value)
        for _type in numeric_types:
            try:
                return _type(value)
            except Exception:
                pass
    except InvalidOperation:
        return raw_value


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
    try:  # pragma: no cover
        import sqlparse

        return sqlparse.format(raw_sql, reindent=reindent)
    except ImportError:  # pragma: no cover
        return raw_sql


def merge_dicts(*dict_args):
    """Merge given dicts into a new dict."""
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def get_table_name(name):
    def _join(match):
        word = match.group()
        if len(word) > 1:
            return ("_%s_%s" % (word[:-1], word[-1])).lower()
        return "_" + word.lower()

    return re.compile(r"([A-Z]+)(?=[a-z0-9])").sub(_join, name).lstrip("_")


def print_query_results(results, name=None):
    if name:
        print(name)
        print("-" * len(name))

    if results:
        colnames = results[0].to_dict().keys()

        # Print header (colname in diagonal)
        nb_lines = len(max(colnames, key=len))
        step = 8
        header = []
        for i in range(nb_lines):
            h = [colname[i] if len(colname) > i else " " for colname in colnames]
            header.append(" " * i + (" " * (step - 1)).join(h))

        for i in reversed(range(nb_lines)):
            print(header[i])

        # Print vlaues
        fmt = ("{:" + str(step) + "." + str(step - 1) + "}") * len(colnames)
        # fmt = * len(colnames)
        # print(fmt.format(*colnames))

        for res in results:
            print(fmt.format(*[str(s) for s in res.to_dict().values()]))
