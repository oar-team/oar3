# -*- coding: utf-8 -*-
import sys


PY3 = sys.version_info[0] == 3


if PY3:
    builtin_str = str
    str = str
    bytes = bytes
    basestring = (str, bytes)
    string_types = (str,)
    numeric_types = (int, float)

    from io import StringIO
    from queue import Empty

    def iterkeys(d):
        return iter(d.keys())

    def itervalues(d):
        return iter(d.values())

    def iteritems(d):
        return iter(d.items())

    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

    def is_bytes(x):
        return isinstance(x, (bytes, memoryview, bytearray))

    from collections import Callable

    def callable(obj):
        return isinstance(obj, Callable)

    # Simple container
    from types import SimpleNamespace

else:
    builtin_str = str
    bytes = str
    str = unicode
    basestring = basestring
    string_types = (unicode, bytes)
    numeric_types = (int, long, float)

    from cStringIO import StringIO  # noqa
    from Queue import Empty  # noqa

    def iterkeys(d):
        return d.iterkeys()

    def itervalues(d):
        return d.itervalues()

    def iteritems(d):
        return d.iteritems()

    exec('def reraise(tp, value, tb=None):\n raise tp, value, tb')

    def is_bytes(x):
        return isinstance(x, (buffer, bytearray))

    callable = callable

    class SimpleNamespace(object):
        """ A generic container for when multiple values need to be returned
        """
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)


def with_metaclass(meta, base=object):
    return meta("NewBase", (base,), {})


def to_unicode(obj, encoding='utf-8'):
    """
    Convert ``obj`` to unicode"""
    # unicode support
    if isinstance(obj, str):
        return obj

    # bytes support
    if is_bytes(obj):
        if hasattr(obj, 'tobytes'):
            return str(obj.tobytes(), encoding)
        return str(obj, encoding)

    # string support
    if isinstance(obj, basestring):
        if hasattr(obj, 'decode'):
            return obj.decode(encoding)
        else:
            return str(obj, encoding)

    return str(obj)
