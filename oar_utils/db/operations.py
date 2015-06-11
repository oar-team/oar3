# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

import sys
import re
from copy import copy
from functools import partial, reduce

from sqlalchemy import (func, MetaData, Table, and_, not_,
                        asc as order_by_func, create_engine)
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import Integer
from sqlalchemy.sql.expression import select
from sqlalchemy_utils.functions import (database_exists, create_database,
                                        render_statement)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError

from oar.lib.compat import to_unicode, iterkeys, reraise

from .helpers import green, magenta, yellow, blue, red
from .alembic import alembic_sync_schema


ARCHIVE_IGNORED_TABLES = [
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


def get_jobs_sync_criterion(ctx, table):
    # prepare query
    criterion = []
    if table.name in jobs_table:
        for column_name in get_jobs_columns(table.name):
            column = table.c.get(column_name)
            criterion.append(column < ctx.max_job_to_sync)
    if table.name in moldable_jobs_tables:
        for column_name in get_moldables_columns(table.name):
            column = table.c.get(column_name)
            criterion.append(column < ctx.max_moldable_job_to_sync)
    return criterion


def get_resources_purge_criterion(ctx, table):
    # prepare query
    criterion = []
    if table.name in resources_tables:
        for column_name in get_resources_columns(table.name):
            column = table.c.get(column_name)
            if ctx.resources_to_purge:
                criterion.append(column.in_(ctx.resources_to_purge))
    return criterion


def archive_db(ctx):
    engine_url = ctx.archive_db.engine.url
    if (not database_exists(engine_url) and is_local_database(ctx, engine_url)
            and ctx.current_db.dialect in ("postgresql", "mysql")):
        clone_db(ctx, ignored_tables=ARCHIVE_IGNORED_TABLES)
        tables = sync_schema(ctx)
        sync_tables(ctx, tables, delete=True,
                    ignored_tables=ARCHIVE_IGNORED_TABLES)
    else:
        if not database_exists(engine_url):
            create_database(engine_url)
        tables = sync_schema(ctx)

        sync_tables(ctx, tables, ignored_tables=ARCHIVE_IGNORED_TABLES)

    fix_sequences(ctx)


def migrate_db(ctx):
    engine_url = ctx.archive_db.engine.url
    if (not database_exists(engine_url) and is_local_database(ctx, engine_url)
            and ctx.current_db.dialect in ("postgresql", "mysql")):
        clone_db(ctx)
    else:
        from oar.lib import models

        from_engine = create_engine(ctx.current_db.engine.url)
        to_engine = create_engine(ctx.archive_db.engine.url)

        # Create databases
        if not database_exists(to_engine.url):
            ctx.log(green(' create') + ' ~> new database `%r`' % to_engine.url)
            create_database(to_engine.url)

        # Create
        tables = [table for name, table in models.all_tables()]
        tables = list(sync_schema(ctx, tables, from_engine, to_engine))

        alembic_sync_schema(ctx, from_engine, to_engine)
        fix_sequences(ctx, to_engine)
        sync_tables(ctx, tables, from_engine=from_engine, to_engine=to_engine)


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
            criterion = get_jobs_sync_criterion(ctx, table)
            query = select([table])
            if criterion:
                query = query.where(reduce(and_, criterion))
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


def sync_schema(ctx, tables=None, from_engine=None, to_engine=None):
    inspector = Inspector.from_engine(ctx.archive_db.engine)
    existing_tables = inspector.get_table_names()
    if tables is None:
        ctx.current_db.reflect()
        tables = ctx.current_db.metadata.sorted_tables
    if to_engine is None:
        to_engine = ctx.archive_db.engine
    if from_engine is None:
        from_engine = ctx.current_db.engine
    for table in tables:
        if table.name not in existing_tables:
            ctx.log(' %s ~> table %s' % (green('create'), table.name))
            try:
                table.create(bind=to_engine, checkfirst=True)
            except Exception as ex:
                ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
    metadata = MetaData(from_engine)
    for table in tables:
        # Make sure we have the good version of the table
        yield Table(table.name, metadata, autoload=True)


def get_primary_keys(table):
    # Deterministic order
    # Order by name and Integer type first
    pks = sorted((c for c in table.c if c.primary_key), key=lambda c: c.name)
    return sorted(pks, key=lambda x: not isinstance(x.type, Integer))


def get_first_primary_key(table):
    for pk in get_primary_keys(table):
        return pk


def sync_tables(ctx, tables, delete=False, ignored_tables=(),
                from_engine=None, to_engine=None):
    # prepare the connection
    if to_engine is None:
        to_engine = ctx.archive_db.engine
    if from_engine is None:
        from_engine = ctx.current_db.engine

    # Get the max pk
    def do_sync(table, from_conn, to_conn):
        if table.name not in ignored_tables:
            criterion = get_jobs_sync_criterion(ctx, table)
            if delete and criterion:
                reverse_criterion = not_(reduce(and_, criterion))
                delete_from_table(ctx, table, to_conn, reverse_criterion)
            else:
                if table.primary_key:
                    pk = get_first_primary_key(table)
                    if isinstance(pk.type, Integer):
                        max_pk_query = select([func.max(pk)])
                        errors_integrity = {}
                        while True:
                            criterion_c = criterion[:]
                            max_pk = to_conn.execute(max_pk_query).scalar()
                            if max_pk is not None:
                                criterion_c.append(pk > max_pk)
                            try:
                                copy_table(ctx, table, from_conn, to_conn,
                                           criterion_c)
                            except IntegrityError:
                                exc_type, exc_value, tb = sys.exc_info()
                                if max_pk in errors_integrity:
                                    reraise(exc_type, exc_value, tb.tb_next)
                                else:
                                    errors_integrity[max_pk] = True
                                    continue
                            break
                    else:
                        merge_table(ctx, table)
                else:
                    delete_from_table(ctx, table, to_conn)
                    copy_table(ctx, table, from_conn, to_conn)

    with from_engine.connect() as from_conn:
        with to_engine.connect() as to_conn:
            for table in tables:
                do_sync(table, from_conn, to_conn)


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


def delete_from_table(ctx, table, raw_conn, criterion=[], message=None):
    if message:
        ctx.log(message)
    delete_query = table.delete()
    if criterion:
        delete_query = delete_query.where(reduce(and_, criterion))
    ctx.log(magenta(' delete') + ' ~> table %s (in progress)' % table.name,
            nl=False)
    count = raw_conn.execute(delete_query).rowcount
    ctx.log(magenta('\r\033[2K delete') + ' ~> table %s (%s)' % (table.name,
                                                                 blue(count)))
    return count


def delete_orphan(ctx, p_table, p_key, f_table, f_key, raw_conn, message=None):
    if message:
        ctx.log(message)
    dialect = raw_conn.engine.dialect.name
    if dialect == 'mysql':
        raw_query = 'DELETE a FROM {f_table} a\n' \
                    'LEFT JOIN {p_table} b\n' \
                    'ON a.{f_key} = b.{p_key}\n' \
                    'WHERE b.{p_key} IS NULL'.format(**locals())
    elif dialect == 'postgresql':
        raw_query = 'DELETE FROM {f_table} a\n' \
                    'USING {p_table} b\n' \
                    'WHERE a.{f_key} = b.{p_key}\n' \
                    'AND b.{p_key} IS NULL'.format(**locals())
    else:
        raw_query = 'DELETE FROM {f_table}\n' \
                    'WHERE {f_key} NOT IN ' \
                    '(SELECT {p_key} from {p_table})'.format(**locals())
    ctx.log(magenta(' delete') + ' ~> table %s (in progress)' % f_table,
            nl=False)
    count = raw_conn.execute(raw_query).rowcount
    ctx.log(magenta('\r\033[2K delete') + ' ~> table %s (%s)' % (f_table,
                                                                 blue(count)))
    return count


def copy_table(ctx, table, from_conn, to_conn, criterion=[]):
    insert_query = table.insert()
    select_table = select([table])
    select_count = select([func.count()]).select_from(table)
    if criterion:
        select_query = select_table.where(reduce(and_, criterion))
        count_query = select_count.where(reduce(and_, criterion))
    else:
        select_query = select_table
        count_query = select_count

    total_lenght = from_conn.execute(count_query).scalar()
    pks = sorted((c for c in table.c if c.primary_key), key=lambda x: x.name)
    select_query = select_query.order_by(
        *(order_by_func(pk) for pk in pks)
    )

    def fetch_stream():
        q = select_query.execution_options(stream_results=True)
        if ctx.disable_pagination:
            result = from_conn.execute(q)
        else:
            q = q.limit(ctx.chunk)
        page = 0
        while True:
            if ctx.disable_pagination:
                rows = result.fetchmany(ctx.chunk)
            else:
                rows = from_conn.execute(q.offset(page * ctx.chunk)).fetchall()
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
            to_conn.execute(insert_query, rows)

        ctx.log("")


def fix_sequences(ctx, to_engine=None, tables=[]):
    if to_engine is None:
        to_engine = ctx.archive_db.engine
    if not tables:
        tables = ctx.current_db.metadata.sorted_tables

    if not to_engine.dialect.name == "postgresql":
        return

    def get_sequences_values():
        for table in tables:
            pks = [c for c in table.c if c.primary_key]
            for pk in pks:
                if not pk.autoincrement:
                    continue
                sequence_name = "%s_%s_seq" % (table.name, pk.name)
                if to_engine.dialect.has_sequence(to_engine, sequence_name):
                    yield sequence_name, pk.name, pk.table.name

    for sequence_name, pk_name, table_name in get_sequences_values():
        ctx.log(green('\r    fix') + ' ~> sequence %s' % sequence_name)
        query = "select setval('%s', max(%s)) from %s"
        try:
            to_engine.execute(query % (sequence_name, pk_name, table_name))
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
    count = 0
    rv = None
    message = "Purge old resources from database :"
    for table in tables:
        if table.name in resources_tables:
            criterion = get_resources_purge_criterion(ctx, table)
            if criterion:
                rv = delete_from_table(ctx, table, raw_conn, criterion,
                                       message)
                if message is not None:
                    message = None
                count += rv
    message = "Purge old jobs from database :"
    for table in tables:
        criterion = get_jobs_sync_criterion(ctx, table)
        if criterion:
            rv = delete_from_table(ctx, table, raw_conn, criterion, message)
            if message is not None:
                message = None
            count += rv
    # Purge events
    message = "Purge orphan events from database :"
    rv = delete_orphan(ctx,
                       "event_logs", "event_id",
                       "event_log_hostnames", "event_id",
                       raw_conn, message)
    count += rv
    message = "Purge orphan resources descriptions from database :"
    rv = delete_orphan(ctx,
                       "job_resource_groups", "res_group_id",
                       "job_resource_descriptions", "res_job_group_id",
                       raw_conn, message)
    count += rv

    return count


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
