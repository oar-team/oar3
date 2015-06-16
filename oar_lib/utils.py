# -*- coding: utf-8 -*-
from __future__ import with_statement, absolute_import, unicode_literals

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


class IterStream(object):
    """Wrap an iterator and give it a stream like interface.
    Snippet from werkzeug project.
    """

    def __new__(cls, gen, sentinel=''):
        self = object.__new__(cls)
        self._gen = gen
        self._buf = None
        self.sentinel = sentinel
        self.pos = 0
        return self

    def __iter__(self):
        return self

    def _mixed_join(self, iterable):
        """concatenate any string type in an intelligent way."""
        iterator = iter(iterable)
        first_item = next(iterator, self.sentinel)
        if isinstance(first_item, bytes):
            return first_item + b''.join(iterator)
        return first_item + u''.join(iterator)

    def _buf_append(self, string):
        '''Replace string directly without appending to an empty string,
        avoiding type issues.'''
        if not self._buf:
            self._buf = string
        else:
            self._buf += string

    def read(self, n=-1):
        if n < 0:
            self._buf_append(self._mixed_join(self._gen))
            result = self._buf[self.pos:]
            self.pos += len(result)
            return result
        new_pos = self.pos + n
        buf = []
        try:
            tmp_end_pos = 0 if self._buf is None else len(self._buf)
            while new_pos > tmp_end_pos or (self._buf is None and not buf):
                item = next(self._gen)
                tmp_end_pos += len(item)
                buf.append(item)
        except StopIteration:
            pass
        if buf:
            self._buf_append(self._mixed_join(buf))

        if self._buf is None:
            return self.sentinel

        new_pos = max(0, new_pos)
        try:
            return self._buf[self.pos:new_pos]
        finally:
            self.pos = min(new_pos, len(self._buf))

    def readline(self):
        raise NotImplementedError()
