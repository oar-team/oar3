# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import re
from copy import copy
from functools import partial, reduce

from sqlalchemy import func, MetaData, Table, and_, not_
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import Integer
from sqlalchemy.sql.expression import select
from sqlalchemy_utils.functions import (database_exists, create_database,
                                        render_statement)
from sqlalchemy.ext.declarative import declarative_base

from oar.lib.compat import itervalues, to_unicode, iterkeys


from .helpers import green, magenta, yellow, blue, red


SYNC_IGNORED_TABLES = [
    'accounting',
    'gantt_jobs_predictions',
    'gantt_jobs_predictions_log',
    'gantt_jobs_predictions_visu',
    'gantt_jobs_resources',
    'gantt_jobs_resources_log',
    'gantt_jobs_resources_visu',
]

JOBS_TABLES = [
    {'challenges': 'job_id'},
    {'event_logs': 'job_id'},
    {'frag_jobs': 'frag_id_job'},
    {'job_dependencies': 'job_id'},
    {'job_dependencies': 'job_id_required'},
    {'job_state_logs': 'job_id'},
    {'job_types': 'job_id'},
    {'jobs': 'job_id'},
    {'moldable_job_descriptions': 'moldable_job_id'},
]

MOLDABLE_JOBS_TABLES = [
    {'assigned_resources': 'moldable_job_id'},
    {'job_resource_groups': 'res_group_moldable_id'},
    {'moldable_job_descriptions': 'moldable_id'},
    {'gantt_jobs_predictions': 'moldable_job_id'},
    {'gantt_jobs_predictions_log': 'moldable_job_id'},
    {'gantt_jobs_predictions_visu': 'moldable_job_id'},
    {'gantt_jobs_resources': 'moldable_job_id'},
    {'gantt_jobs_resources_log': 'moldable_job_id'},
    {'gantt_jobs_resources_visu': 'moldable_job_id'},
]

RESOURCES_TABLES = [
    {'assigned_resources': 'resource_id'},
    {'resource_logs': 'resource_id'},
    {'resources': 'resource_id'},
    {'gantt_jobs_resources': 'resource_id'},
    {'gantt_jobs_resources_log': 'resource_id'},
    {'gantt_jobs_resources_visu': 'resource_id'},
]

jobs_table = [list(d.keys())[0] for d in JOBS_TABLES]
moldable_jobs_tables = [list(d.keys())[0] for d in MOLDABLE_JOBS_TABLES]
resources_tables = [list(d.keys())[0] for d in RESOURCES_TABLES]


def get_table_columns(tables, table_name):
    return [d[table_name] for d in tables if table_name in d.keys()]


get_jobs_columns = partial(get_table_columns, JOBS_TABLES)
get_moldables_columns = partial(get_table_columns, MOLDABLE_JOBS_TABLES)
get_resources_columns = partial(get_table_columns, RESOURCES_TABLES)


def get_jobs_sync_criteria(ctx, table):
    # prepare query
    criteria = []
    if table.name in jobs_table:
        for column_name in get_jobs_columns(table.name):
            column = table.c.get(column_name)
            criteria.append(column < ctx.max_job_to_sync)
    if table.name in moldable_jobs_tables:
        for column_name in get_moldables_columns(table.name):
            column = table.c.get(column_name)
            criteria.append(column < ctx.max_moldable_job_to_sync)
    return criteria


def get_resources_purge_criteria(ctx, table):
    # prepare query
    criteria = []
    if table.name in resources_tables:
        for column_name in get_resources_columns(table.name):
            column = table.c.get(column_name)
            if ctx.resources_to_purge:
                criteria.append(column.in_(ctx.resources_to_purge))
    return criteria


def archive_db(ctx):
    engine_url = ctx.archive_db.engine.url
    if (not database_exists(engine_url) and is_local_database(ctx, engine_url)
            and ctx.current_db.dialect in ("postgresql", "mysql")):
        clone_db(ctx, ignored_tables=SYNC_IGNORED_TABLES)
        tables = sync_schema(ctx)
        sync_tables(ctx, tables, delete=True)
    else:
        tables = sync_schema(ctx)
        sync_tables(ctx, tables)

    if ctx.archive_db.dialect == "postgresql":
        fix_sequences(ctx)


def migrate_db(ctx):
    engine_url = ctx.archive_db.engine.url
    if (not database_exists(engine_url) and is_local_database(ctx, engine_url)
            and ctx.current_db.dialect in ("postgresql", "mysql")):
        clone_db(ctx)
    else:
        tables = sync_schema(ctx)
        raw_conn = ctx.archive_db.engine.connect()
        for table in tables:
            copy_table(ctx, table, raw_conn)
        if ctx.archive_db.dialect == "postgresql":
            fix_sequences(ctx)


def reflect_table(db, table_name):
    metadata = MetaData(db.engine)
    return Table(table_name, metadata, autoload=True)


def clone_db(ctx, ignored_tables=()):
    message = ' ~> `%s` to `%s` database' % (ctx.current_db_name,
                                             ctx.archive_db_name)
    ctx.log(green('  clone') + message)
    if ctx.current_db.dialect == 'postgresql':
        ctx.current_db.session.connection().connection.set_isolation_level(0)
        ctx.current_db.session.execute(
            'CREATE DATABASE "%s" WITH TEMPLATE "%s";'
            % (ctx.archive_db_name, ctx.current_db_name)
        )
        ctx.current_db.session.connection().connection.set_isolation_level(1)
    elif ctx.current_db.dialect == 'mysql':
        # Horribly slow implementation.
        create_database(ctx.archive_db.engine.url)
        show_tables_query = 'SHOW TABLES in %s;' % ctx.current_db_name
        for row in ctx.current_db.session.execute(show_tables_query):
            name = row[0]
            ctx.current_db.session.execute(
                'CREATE TABLE %s.%s LIKE %s.%s'
                % (ctx.archive_db_name, name, ctx.current_db_name, name)
            )
            if name in ignored_tables:
                ctx.log(yellow(' ignore') + ' ~> table %s' % name)
                continue
            ctx.current_db.session.execute(
                'ALTER TABLE %s.%s DISABLE KEYS' % (ctx.archive_db_name, name)
            )
            table = reflect_table(ctx.current_db, name)
            criteria = get_jobs_sync_criteria(ctx, table)
            query = select([table])
            if criteria:
                query = query.where(reduce(and_, criteria))
            raw_sql = render_statement(query, ctx.current_db.engine)
            raw_sql = raw_sql.replace(";", "").replace("\n", "")
            matcher = re.compile(r'SELECT(.*?)FROM', re.IGNORECASE | re.DOTALL)
            result = matcher.search(raw_sql)
            if result:
                raw_sql = raw_sql.replace(result.group(1), " * ")
            ctx.current_db.session.execute(
                'INSERT INTO %s.%s (%s)' % (ctx.archive_db_name, name, raw_sql)
            )
            ctx.current_db.session.execute(
                'ALTER TABLE %s.%s ENABLE KEYS' % (ctx.archive_db_name, name)
            )
    else:
        raise NotSupportedDatabase()


def sync_schema(ctx):
    ctx.current_db.reflect()
    inspector = Inspector.from_engine(ctx.archive_db.engine)
    existing_tables = inspector.get_table_names()
    for table in ctx.current_db.metadata.sorted_tables:
        if table.name not in existing_tables:
            ctx.log(' %s ~> table %s' % (green('create'), table.name))
            try:
                table.create(bind=ctx.archive_db.engine, checkfirst=True)
            except Exception as ex:
                ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
    metadata = MetaData(ctx.current_db.engine)
    for table in ctx.current_db.metadata.sorted_tables:
        # Make sure we have the good version of the table
        yield Table(table.name, metadata, autoload=True)


def sync_tables(ctx, tables, delete=False):
    # prepare the connection
    raw_conn = ctx.archive_db.engine.connect()
    # Get the max pk
    for table in tables:
        if table.name not in SYNC_IGNORED_TABLES:
            criteria = get_jobs_sync_criteria(ctx, table)
            if delete and criteria:
                reverse_criteria = [not_(c) for c in criteria]
                delete_from_table(ctx, table, raw_conn, reverse_criteria)
            else:
                if table.primary_key:
                    pk = table.primary_key.columns.values()[0]
                    if isinstance(pk.type, Integer):
                        max_pk_query = select([func.max(pk)])
                        max_pk = raw_conn.execute(max_pk_query).scalar()
                        if max_pk is not None:
                            criteria.append(pk > max_pk)
                        copy_table(ctx, table, raw_conn, criteria)
                    else:
                        merge_table(ctx, table)
                else:
                    delete_from_table(ctx, table, raw_conn)
                    copy_table(ctx, table, raw_conn)


def merge_table(ctx, table):
    # Very slow !!
    session = ctx.archive_db.session
    ctx.log(' %s ~> table %s' % (magenta(' merge'), table.name))
    model = generic_mapper(table)
    columns = table.columns.keys()
    for record in ctx.current_db.query(table).all():
        data = dict(
            [(str(column), getattr(record, column)) for column in columns]
        )
        session.merge(model(**data))
    session.commit()


def delete_from_table(ctx, table, raw_conn, criteria=[], message=None):
    check_query = select([func.count()]).select_from(table)
    if criteria:
        check_query = check_query.where(reduce(and_, criteria))
    count = raw_conn.execute(check_query).scalar()
    if count > 0:
        if message:
            ctx.log(message)
        delete_query = table.delete()
        count_str = blue("%s" % (count))
        ctx.log(magenta(' delete') + ' ~> table %s (%s)' % (table.name,
                                                            count_str))
        if criteria:
            delete_query = delete_query.where(reduce(and_, criteria))
        return raw_conn.execute(delete_query)


def copy_table(ctx, table, raw_conn, criteria=[]):
    # prepare the connection
    from_conn = ctx.current_db.engine.connect()

    insert_query = table.insert()
    select_table = select([table])
    select_count = select([func.count()]).select_from(table)
    if criteria:
        select_query = select_table.where(reduce(and_, criteria))
        count_query = select_count.where(reduce(and_, criteria))
    else:
        select_query = select_table
        count_query = select_count

    total_lenght = from_conn.execute(count_query).scalar()

    def fetch_stream():
        q = select_query.execution_options(stream_results=True)
        page = 0
        result = from_conn.execute(q)
        while True:
            rows = result.fetchmany(ctx.chunk)
            if not rows:
                break
            yield rows
            page = page + 1

    if total_lenght > 0:
        message = yellow('\r   copy') + ' ~> table %s (%s)'
        ctx.log(message % (table.name, blue("0/%s" % total_lenght)), nl=False)
        progress = 0
        for rows in fetch_stream():
            progress = ctx.chunk + progress
            progress = total_lenght if progress > total_lenght else progress
            percentage = blue("%s/%s" % (progress, total_lenght))
            ctx.log(message % (table.name, percentage), nl=False)
            raw_conn.execute(insert_query, rows)

        ctx.log("")


def fix_sequences(ctx):
    engine = ctx.archive_db.engine

    def get_sequences_values():
        for model in itervalues(ctx.current_models):
            for pk in model.__mapper__.primary_key:
                if not pk.autoincrement:
                    continue
                sequence_name = "%s_%s_seq" % (pk.table.name, pk.name)
                if engine.dialect.has_sequence(engine, sequence_name):
                    yield sequence_name, pk.name, pk.table.name

    for sequence_name, pk_name, table_name in get_sequences_values():
        ctx.log(green('\r    fix') + ' ~> sequence %s' % sequence_name)
        query = "select setval('%s', max(%s)) from %s"
        try:
            engine.execute(query % (sequence_name, pk_name, table_name))
        except Exception as ex:
            ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
    ctx.archive_db.commit()


def generic_mapper(table):
    class GenericMapper(declarative_base()):
        __table__ = table
    return GenericMapper


def is_local_database(ctx, engine_url):
    url = copy(engine_url)
    url.database = ctx.current_db.engine.url.database
    return url == ctx.current_db.engine.url


class NotSupportedDatabase(Exception):
    pass


def purge_db(ctx):
    # prepare the connection
    db = ctx.current_db
    raw_conn = db.engine.connect()
    inspector = Inspector.from_engine(db.engine)
    tables = [reflect_table(db, name) for name in inspector.get_table_names()]
    tables_dict = dict(((t.name, t) for t in tables))
    change = False
    rv = None
    message = "Purge old resources from database :"
    for table in tables:
        if table.name in resources_tables:
            criteria = get_resources_purge_criteria(ctx, table)
            if criteria:
                rv = delete_from_table(ctx, table, raw_conn, criteria, message)
                if message is not None:
                    message = None
                if not change and rv is not None:
                    change = True
    message = "Purge old jobs from database :"
    for table in tables:
        criteria = get_jobs_sync_criteria(ctx, table)
        if criteria:
            rv = delete_from_table(ctx, table, raw_conn, criteria, message)
            if message is not None:
                message = None
            if not change and rv is not None:
                change = True
    # Purge events
    message = "Purge old events from database :"
    event_log_hostnames = tables_dict["event_log_hostnames"]
    event_logs = tables_dict["event_logs"]
    t = select([event_logs.c["event_id"]])
    criteria = [event_log_hostnames.c.get("event_id").notin_(t)]
    rv = delete_from_table(ctx, event_log_hostnames, raw_conn,
                           criteria, message)
    if not change and rv is not None:
        change = True
    message = "Purge old resources descriptions from database :"
    resource_descriptions = tables_dict["job_resource_descriptions"]
    resource_groups = tables_dict["job_resource_groups"]
    t = select([resource_groups.c["res_group_id"]])
    criteria = [resource_descriptions.c.get("res_job_group_id").notin_(t)]
    rv = delete_from_table(ctx, resource_descriptions, raw_conn,
                           criteria, message)
    if not change and rv is not None:
        change = True

    return change


def count_all(db):
    # prepare the connection
    raw_conn = db.engine.connect()
    inspector = Inspector.from_engine(db.engine)
    tables = [reflect_table(db, name) for name in inspector.get_table_names()]
    for table in tables:
        count_query = select([func.count()]).select_from(table)
        yield table.name, raw_conn.execute(count_query).scalar()


def inspect_db(ctx):
    infos = dict()
    for table_name, size in count_all(ctx.current_db):
        infos[table_name] = {"current_db_size": size,
                             "archive_db_size": 0,
                             "diff": size}
    if database_exists(ctx.archive_db.engine.url):
        for table_name, size in count_all(ctx.archive_db):
            infos[table_name]["archive_db_size"] = size
            diff = infos[table_name]["current_db_size"] - size
            infos[table_name]["diff"] = diff

    headers = ["Table", "Current DB size", "Archive DB size", "Diff"]
    rows = [(k,
             to_unicode(infos[k]["current_db_size"]),
             to_unicode(infos[k]["archive_db_size"]),
             to_unicode(infos[k]["diff"]))
            for k in iterkeys(infos)]
    return sorted(rows, key=lambda x: x[0]), headers
