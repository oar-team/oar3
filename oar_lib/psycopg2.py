# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import random
from struct import pack

from sqlalchemy import types as sa_types
from oar.lib.compat import is_bytes, str
from oar.lib.utils import IterStream


def serialize_rows_to_csv(rows, null_value):
    def escape_row(row):
        for value in row:
            if value is None:
                yield null_value
            elif isinstance(value, basestring):
                if '\n' in value or '\t' in value or '\r' in value:
                    yield value.replace('\n', '\\n')\
                               .replace('\t', '\\t')\
                               .replace('\r', '\\r')
                else:
                    yield value
            else:
                yield value.__str__()

    for row in rows:
        yield "\t".join(escape_row(row)) + "\n"


def sqlalchemy_struct_mapper_type(columns):
    type_map = [
        (sa_types.SmallInteger, b'h', 2),
        (sa_types.BigInteger, b'q', 8),
        (sa_types.Integer, b'i', 4),
        (sa_types.Boolean, b'b', 1),
        (sa_types.Float, b'f', 4),
    ]
    for colums in columns:
        fmt_and_size = None
        for t, fmt, size in type_map:
            if isinstance(colums.type, t):
                fmt_and_size = (fmt, size)
            if fmt_and_size is not None:
                yield fmt_and_size
                break

        if fmt_and_size is None:
            yield (None, None)
            fmt_and_size = None


def serialize_rows_to_binary(rows, columns_obj):
    yield pack(b'!11sii', b'PGCOPY\n\377\r\n\0', 0, 0)

    formats = list(sqlalchemy_struct_mapper_type(columns_obj))
    for row in rows:
        yield pack(b'!h', len(columns_obj))
        for (fmt, size), value in zip(formats, row):
            if value is None:
                size = -1
                yield pack(b'!i', size)
            else:
                if fmt is None:
                    if is_bytes(value):
                        size = len(value)
                    elif isinstance(value, str):
                        value = value.encode('utf-8')
                        size = len(value)
                    else:
                        value = ("%s" % value).encode('utf-8')
                        size = len(value)
                    fmt = ("%ds" % size).encode('utf-8')
                yield pack(b'!i' + fmt, size, value)

    yield pack(b'!h', -1)


def pg_bulk_insert(cursor, table, rows, columns, binary=True):

    if binary:
        columns_obj = [table.columns[key] for key in columns]
        binary_stream = IterStream(serialize_rows_to_binary(rows, columns_obj))
        query = "COPY %s(%s) FROM STDIN WITH BINARY" % (table.name,
                                                        ", ".join(columns))
        cursor.copy_expert(query, binary_stream)
    else:
        null_value = 'None_%x' % random.getrandbits(32)
        csv_stream = IterStream(serialize_rows_to_csv(rows, null_value))
        cursor.copy_from(csv_stream,
                         "%s" % table.name,
                         columns=columns,
                         null=null_value)
