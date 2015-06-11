# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import random
from multiprocessing import Process
from oar.lib.utils import make_temp_fifo


def serialize_result_proxy_to_csv(rows, null_value, output):
    def escape_row(row):
        for value in row.values():
            if value is None:
                value = null_value
            elif isinstance(value, basestring):
                if '\n' in value or '\r' in value or '\t' in value:
                    value = value.replace('\r', '\\r')\
                                 .replace('\n', '\\n')\
                                 .replace('\t', '\\t')
            else:
                value = "%s" % value
            yield value

    for row in rows:
        output.write("\t".join(escape_row(row)) + "\n")


def pg_bulk_insert(cursor, table, rows, columns):
    with make_temp_fifo(table.name) as fifo:

        null_value = 'None_%x' % random.getrandbits(32)

        def writer_process():
            with open(fifo, 'w') as fifo_output:
                serialize_result_proxy_to_csv(rows, null_value, fifo_output)

        writer = Process(target=writer_process)
        writer.start()

        with open(fifo, 'r') as fifo_input:
            cursor.copy_from(fifo_input,
                             "%s" % table.name,
                             columns=columns,
                             null=null_value)
        writer.join()
