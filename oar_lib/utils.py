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


