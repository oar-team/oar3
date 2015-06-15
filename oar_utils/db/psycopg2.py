# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import random

from io import open
from multiprocessing import Process

from oar.lib.utils import make_temp_fifo
from werkzeug.contrib.iterio import IterIO


def serialize_result_proxy_to_csv(rows, null_value, output_file):
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


def pg_bulk_insert(cursor, table, rows, columns):
    null_value = 'None_%x' % random.getrandbits(32)

    iter_stream = IterIO(serialize_result_proxy_to_csv(rows, null_value))
    cursor.copy_from(iter_stream,
                     "%s" % table.name,
                     columns=columns,
                     null=null_value)
