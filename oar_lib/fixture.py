# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

import re
import time

from codecs import open
from collections import OrderedDict
from datetime import datetime

from .compat import json, iteritems, itervalues, iterkeys
from .utils import JSONEncoder, ResultProxyIter


class JsonSerializer(object):

    def __init__(self, filename, ref_time=None):
        self.filename = filename
        self.ref_time = ref_time

    @property
    def time_offset(self):
        if self.ref_time is None:
            return 0
        else:
            return self.ref_time - self.old_ref_time

    def convert_datetime(self, dct):
        for key, value in iteritems(dct):
            try:
                dct[key] = datetime(*map(int, re.split('[^\d]', value)[:-1]))
            except Exception:
                pass
        return dct

    def load(self, time_columns=()):
        self.old_ref_time = int(time.time())
        with open(self.filename, 'r', encoding='utf-8') as fd:
            dct = json.load(fd, object_hook=self.convert_datetime)
            self.old_ref_time = dct['metadata']['ref_time']
            if self.time_offset != 0:
                for data in dct['data']:
                    for record in data['records']:
                        for key, value in record.items():
                            if (key in time_columns
                                    and 0 < record[key] < 2147483647):
                                record[key] = record[key] + self.time_offset
            return dct['data']

    def dump(self, data):
        obj = OrderedDict()
        obj['metadata'] = {'ref_time': self.ref_time}
        obj['data'] = data
        with open(self.filename, 'w', encoding='utf-8') as fd:
            kwargs = {
                'ensure_ascii': True,
                'cls': JSONEncoder,
                'indent': 2,
                'separators': (',', ': ')
            }
            json.dump(obj, fd, **kwargs)


def get_defined_tables(db):
    all_tables = set([table_name for table_name in iterkeys(db.tables)])
    tables_from_models = set([m.__table__.name for m in itervalues(db.models)])
    tables_only = list(all_tables - tables_from_models)
    tables = {}
    for table_name in tables_only:
        tables[table_name] = db[table_name]
    return tables


def load_fixtures(db, filename, ref_time=None, clear=False, time_columns=()):
    time_columns = time_columns or getattr(db, '__time_columns__', [])
    data = JsonSerializer(filename, ref_time).load(time_columns)
    if clear:
        db.delete_all()
    for fixture in data:
        if "table" in fixture:
            table = db[fixture['table']]
            db.session.execute(table.insert(), fixture['records'])
        else:
            model = db[fixture['model']]
            db.session.bulk_insert_mappings(model, fixture['records'])
        db.commit()


def dump_fixtures(db, filename, ref_time=None):
    ref_time = int(time.time()) if ref_time is None else ref_time
    tables = get_defined_tables(db)
    data = []
    for table_name, table in iteritems(tables):
        entry = OrderedDict()
        entry['table'] = table_name
        entry['records'] = ResultProxyIter(db.session.execute(table.select()))
        data.append(entry)
    for model_name, model in iteritems(db.models):
        entry = OrderedDict()
        entry['model'] = model_name
        entry['records'] = db.query(model).all()
        data.append(entry)

    JsonSerializer(filename, ref_time).dump(data)
