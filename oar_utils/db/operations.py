# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals

from copy import copy

from sqlalchemy import func, MetaData, Table
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import Integer
from sqlalchemy.sql.expression import select
from sqlalchemy_utils.functions import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base

from oar.lib.compat import  itervalues, to_unicode


from .helpers import green, magenta, yellow, blue, red


class NotSupportedDatabase(Exception):
    pass


UNUSED_TABLES = (
    'accounting',
    'gantt_jobs_predictions',
    'gantt_jobs_predictions_log',
    'gantt_jobs_predictions_visu',
    'gantt_jobs_resources',
    'gantt_jobs_resources_log',
    'gantt_jobs_resources_visu',
)


def copy_db(ctx):
    engine_url = ctx.archive_db.engine.url
    if (not database_exists(engine_url) and is_local_database(ctx, engine_url)
        and ctx.current_db.dialect in ("postgresql", "mysql")):
            clone_db(ctx)
    else:
        tables = sync_schema(ctx)
        sync_tables(ctx, tables)
        if ctx.current_db.dialect == "postgresql":
            fix_sequences(ctx)


def clone_db(ctx):
    message = ' ~> `%s` to `%s` database' % (ctx.current_db_name,
                                             ctx.archive_db_name)
    ctx.log(green('  clone') + message)
    if ctx.current_db.dialect == 'postgresql':
        ctx.current_db.session.connection().connection.set_isolation_level(0)
        ctx.current_db.session.execute(
            '''
                CREATE DATABASE "%s" WITH TEMPLATE "%s";
            ''' %
            (
                ctx.archive_db_name,
                ctx.current_db_name
            )
        )
        ctx.current_db.session.connection().connection.set_isolation_level(1)
    elif ctx.current_db.dialect == 'mysql':
        # Horribly slow implementation.
        create_database(ctx.archive_db.engine.url)
        show_tables_query = 'SHOW TABLES in %s;' % ctx.current_db_name
        for row in ctx.current_db.session.execute(show_tables_query):
            if row[0] in UNUSED_TABLES:
                ctx.log(yellow(' ignore') + ' ~> table %s' % row[0])
            ctx.current_db.session.execute('''
                CREATE TABLE %s.%s LIKE %s.%s
            ''' % (
                ctx.archive_db_name,
                row[0],
                ctx.current_db_name,
                row[0]
            ))
            ctx.current_db.session.execute('ALTER TABLE %s.%s DISABLE KEYS' % (
                ctx.archive_db_name,
                row[0]
            ))
            ctx.current_db.session.execute('''
                INSERT INTO %s.%s SELECT * FROM %s.%s
            ''' % (
                ctx.archive_db_name,
                row[0],
                ctx.current_db_name,
                row[0]
            ))
            ctx.current_db.session.execute('ALTER TABLE %s.%s ENABLE KEYS' % (
                ctx.archive_db_name,
                row[0]
            ))
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
                yield table
            except Exception as ex:
                ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
        else:
            # Make sure we have the good version of the table
            metadata = MetaData(ctx.current_db.engine)
            yield Table(table.name, metadata, autoload=True)


def sync_tables(ctx, tables):
    # prepare the connection
    raw_conn = ctx.archive_db.engine.connect()
    # Get the max pk
    for table in tables:
        if table.name not in UNUSED_TABLES:
            if table.primary_key:
                pk = table.primary_key.columns.values()[0]
                if isinstance(pk.type, Integer):
                    max_pk_query = select([func.max(pk)])
                    max_pk = raw_conn.execute(max_pk_query).scalar()
                    cond = None
                    if max_pk is not None:
                        cond = (pk > max_pk)
                    copy_table(ctx, table, raw_conn, condition=cond)
                else:
                    merge_table(ctx, table)
            else:
                delete_from_tables(ctx, table, raw_conn)
                copy_table(ctx, table, raw_conn)


def merge_table(ctx, table):
    ## Very slow !!
    session = ctx.archive_db.session
    ctx.log(' %s ~> table %s' % (magenta(' merge'), table.name))
    Model = generic_mapper(table)
    columns = table.columns.keys()
    for record in ctx.current_db.query(table).all():
        data = dict(
            [(str(column), getattr(record, column)) for column in columns]
        )
        session.merge(Model(**data))
    session.commit()


def delete_from_tables(ctx, table, raw_conn, condition=None):
    delete_query = table.delete()
    if condition is not None:
        ctx.log(magenta(' delete') + ' ~> from table ' + table.name)
        delete_query = delete_query.where(condition)
    else:
        ctx.log(magenta('  empty') + ' ~> table ' + table.name)
    raw_conn.execute(delete_query)


def copy_table(ctx, table, raw_conn, condition=None):
    # prepare the connection
    from_conn = ctx.current_db.engine.connect()

    insert_query = table.insert()
    select_table = select([table])
    select_count = select([func.count()]).select_from(table)
    if condition is not None:
        select_query = select_table.where(condition)
        count_query = select_count.where(condition)
    else:
        select_query = select_table
        count_query = select_count

    total_lenght = from_conn.execute(count_query).scalar()
    result = from_conn.execution_options(stream_results=True)\
                            .execute(select_query)
    message = yellow('\r   copy') + ' ~> table %s (%s)'
    ctx.log(message % (table.name, blue("0/%s" % total_lenght)), nl=False)
    if total_lenght > 0:
        progress = 0
        while True:
            transaction = raw_conn.begin()
            rows = result.fetchmany(ctx.chunk)
            lenght = len(rows)
            if lenght == 0:
                break
            progress = lenght + progress
            percentage = blue("%s/%s" % (progress, total_lenght))
            ctx.log(message % (table.name, percentage), nl=False)
            raw_conn.execute(insert_query, rows)
            del rows
        transaction.commit()
    ctx.log("")


def fix_sequences(ctx):
    engine = ctx.archive_db.engine
    def get_sequences_values():
        for model in itervalues(ctx.current_db.models):
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
    Base = declarative_base()
    class GenericMapper(Base):
        __table__ = table
    return GenericMapper


def is_local_database(ctx, engine_url):
    url = copy(engine_url)
    url.database = ctx.current_db.engine.url.database
    return url == ctx.current_db.engine.url
