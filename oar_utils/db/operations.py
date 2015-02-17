# -*- coding: utf-8 -*-
from __future__ import division

from copy import copy

from sqlalchemy import func
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.types import Integer
from sqlalchemy.sql.expression import select
from sqlalchemy_utils.functions import database_exists, create_database
from sqlalchemy.ext.declarative import declarative_base

from oar.lib import db, Database
from oar.lib.compat import  itervalues, to_unicode


from .helpers import log, green, magenta, yellow, blue, red


class NotSupportedDatabase(Exception):
    pass


def local_database(engine_url):
    url = copy(engine_url)
    url.database = db.engine.url.database
    return url == db.engine.url


def sync_db(to_db_url, chunk_size=1000):
    to_db = Database(uri=to_db_url)
    engine_url = to_db.engine.url
    if not database_exists(engine_url) and local_database(engine_url):
        if db.engine.dialect.name in ("postgresql", "mysql"):
            copy_db(to_db)

    tables = sync_schema(to_db)
    sync_tables(tables, to_db, chunk_size)
    fix_sequences(to_db)


def sync_schema(to_db):
    db.reflect()
    existing_tables = Inspector.from_engine(to_db.engine).get_table_names()
    for table in db.metadata.sorted_tables:
        if table.name not in existing_tables:
            log(' %s ~> table %s' % (green('create'), table.name))
            try:
                table.create(bind=to_db.engine, checkfirst=True)
                yield table
            except Exception as ex:
                log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
        else:
            # Make sure we have the good version of the table
            yield db.Table(table.name, db.MetaData(db.engine), autoload=True)


def sync_tables(tables, to_db, chunk_size):
    # prepare the connection
    raw_conn = to_db.engine.connect()
    # Get the max pk
    for table in tables:
        if table.primary_key:
            pk = table.primary_key.columns.values()[0]
            if isinstance(pk.type, Integer):
                max_pk_query = select([func.max(pk)])
                max_pk = raw_conn.execute(max_pk_query).scalar()
                cond = None
                if max_pk is not None:
                    cond = (pk > max_pk)
                copy_table(table, raw_conn, chunk_size, condition=cond)
            else:
                merge_table(table, to_db.session)
        else:
            log(magenta('  empty') + ' ~> table ' + table.name)
            delete_from_tables(table, raw_conn)
            copy_table(table, raw_conn, chunk_size)


def merge_table(table, session):
    ## Very slow !!
    log(' %s ~> table %s' % (magenta(' merge'), table.name))
    Model = generic_mapper(table)
    columns = table.columns.keys()
    for record in db.query(table).all():
        data = dict(
            [(str(column), getattr(record, column)) for column in columns]
        )
        session.merge(Model(**data))
    session.commit()


def delete_from_tables(table, raw_conn, condition=None):
    delete_query = table.delete()
    if condition is not None:
        delete_query = delete_query.where(condition)
    raw_conn.execute(delete_query)


def copy_table(table, raw_conn, chunk_size, condition=None):
    # prepare the connection
    from_conn = db.engine.connect()

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
    log(message % (table.name, blue("0/%s" % total_lenght)), nl=False)
    if total_lenght > 0:
        progress = 0
        while True:
            transaction = raw_conn.begin()
            rows = result.fetchmany(chunk_size)
            lenght = len(rows)
            if lenght == 0:
                break
            progress = lenght + progress
            percentage = blue("%s/%s" % (progress, total_lenght))
            log(message % (table.name, percentage), nl=False)
            raw_conn.execute(insert_query, rows)
            del rows
        transaction.commit()
    log("")


def fix_sequences(to_db):
    engine = to_db.engine
    def get_sequences_values():
        for model in itervalues(db.models):
            for pk in model.__mapper__.primary_key:
                if not pk.autoincrement:
                    continue
                sequence_name = "%s_%s_seq" % (pk.table.name, pk.name)
                if engine.dialect.has_sequence(engine, sequence_name):
                    yield sequence_name, pk.name, pk.table.name

    if to_db.engine.url.get_dialect().name == "postgresql":
        for sequence_name, pk_name, table_name in get_sequences_values():
            log(green('\r    fix') + ' ~> sequence %s' % sequence_name)
            query = "select setval('%s', max(%s)) from %s"
            try:
                engine.execute(query % (sequence_name, pk_name, table_name))
            except Exception as ex:
                log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
        to_db.commit()


def copy_db(to_db):
    from_db_name = db.engine.url.database
    to_db_name = to_db.engine.url.database
    log(green('  clone') + ' ~> `%s` to `%s` database' % (from_db_name,
                                                          to_db_name))
    if db.engine.dialect.name == 'postgresql':
        db.session.connection().connection.set_isolation_level(0)
        db.session.execute(
            '''
                CREATE DATABASE "%s" WITH TEMPLATE "%s";
            ''' %
            (
                to_db_name,
                from_db_name
            )
        )
        db.session.connection().connection.set_isolation_level(1)
    elif db.engine.dialect.name == 'mysql':
        # Horribly slow implementation.
        create_database(to_db.engine.url)
        for row in db.session.execute('SHOW TABLES in %s;' % from_db_name):
            db.session.execute('''
                CREATE TABLE %s.%s LIKE %s.%s
            ''' % (
                to_db_name,
                row[0],
                from_db_name,
                row[0]
            ))
            db.session.execute('ALTER TABLE %s.%s DISABLE KEYS' % (
                to_db_name,
                row[0]
            ))
            db.session.execute('''
                INSERT INTO %s.%s SELECT * FROM %s.%s
            ''' % (
                to_db_name,
                row[0],
                from_db_name,
                row[0]
            ))
            db.session.execute('ALTER TABLE %s.%s ENABLE KEYS' % (
                to_db_name,
                row[0]
            ))
    else:
        raise NotSupportedDatabase()


def generic_mapper(table):
    Base = declarative_base()
    class GenericMapper(Base):
        __table__ = table
    return GenericMapper
