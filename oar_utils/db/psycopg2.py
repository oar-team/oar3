# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import random

from io import open
from multiprocessing import Process

from oar.lib.utils import make_temp_fifo


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

    with open(output_file, 'w', encoding="utf-8") as output:
        for row in rows:
            output.write("\t".join(escape_row(row)) + "\n")


def pg_bulk_insert(cursor, table, rows, columns):
    with make_temp_fifo(table.name) as fifo:

        null_value = 'None_%x' % random.getrandbits(32)

        def writer_process():
            serialize_result_proxy_to_csv(rows, null_value, fifo)

        writer = Process(target=writer_process)
        writer.start()

        with open(fifo, 'r') as fifo_input:
            cursor.copy_from(fifo_input,
                             "%s" % table.name,
                             columns=columns,
                             null=null_value)
        writer.join()
