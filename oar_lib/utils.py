# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import, unicode_literals

from .compat import numeric_types


def try_convert_decimal(value):
    """ Try to convert ``value`` to a decimal."""
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
    """ A property that is only computed once per instance and then replaces
    itself with an ordinary attribute. Deleting the attribute resets the
    property """

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
