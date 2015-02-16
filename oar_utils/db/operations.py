# -*- coding: utf-8 -*-
from __future__ import division

from copy import copy

from sqlalchemy import func
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.expression import select
from sqlalchemy_utils.functions import database_exists, create_database

from oar.lib import db, Database
from oar.lib.compat import iteritems, itervalues, to_unicode


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
    dialect = db.engine.dialect.name
    if not database_exists(engine_url) and local_database(engine_url):
        if dialect in ("postgresql", "mysql"):
            copy_db(to_db)
    create_all_tables(to_db)
    copy_tables(to_db, chunk_size)
    update_sequences(to_db)


def create_all_tables(to_db):
    db.reflect()
    db.create_all()
    inspector = Inspector.from_engine(to_db.engine)
    existing_tables = inspector.get_table_names()
    for table in db.metadata.sorted_tables:
        if table.name not in existing_tables:
            log(' %s ~> table %s' % (green('create'), table.name))
            try:
                table.create(bind=to_db.engine, checkfirst=True)
            except Exception as ex:
                log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))


def copy_tables(to_db, chunk_size):
    for name, Model in iteritems(db.models):
        copy_model(Model, to_db, chunk_size)
    # Tables without primary keys
    tables_with_pk = (model.__table__ for model in itervalues(db.models))
    all_tables = (table for table in itervalues(db.tables))
    for table in set(all_tables) - set(tables_with_pk):
        empty_table(table, to_db)
        copy_table(table, to_db, chunk_size)


def merge_model(Model, to_db):
    log(' %s ~> table %s' % (magenta(' merge'), Model.__table__.name))
    query_result = db.query(Model)
    for r in query_result:
         to_db.session.merge(r)
    to_db.session.commit()


def copy_model(Model, to_db, chunk_size):
    # prepare the connection
    to_connection = to_db.engine.connect()
    # Get the max pk
    pk = Model.__mapper__.primary_key[0]
    max_pk_query = select([func.max(pk)])
    max_pk = to_connection.execute(max_pk_query).scalar()
    condition = None
    if max_pk is not None:
        condition = (pk > max_pk)
    table = Model.__table__
    copy_table(table, to_db, chunk_size, select_condition=condition)


def copy_table(table, to_db, chunk_size, select_condition=None):
    # prepare the connection
    to_connection = to_db.engine.connect()
    from_connection = db.engine.connect()

    insert_query = table.insert()
    select_table = select([table])
    select_count = select([func.count()]).select_from(table)
    if select_condition is not None:
        select_query = select_table.where(select_condition)
        count_query = select_count.where(select_condition)
    else:
        select_query = select_table
        count_query = select_count

    total_lenght = from_connection.execute(count_query).scalar()
    result = from_connection.execution_options(stream_results=True)\
                            .execute(select_query)

    message = yellow('\r   copy') + ' ~> table %s (%s)'
    log(message % (table.name, blue("0/%s" % total_lenght)), nl=False)
    if total_lenght > 0:
        progress = 0
        while True:
            transaction = to_connection.begin()
            rows = result.fetchmany(chunk_size)
            lenght = len(rows)
            if lenght == 0:
                break
            progress = lenght + progress
            percentage = blue("%s/%s" % (progress, total_lenght))
            log(message % (table.name, percentage), nl=False)
            to_connection.execute(insert_query, rows)
            del rows
            transaction.commit()
    log("")


def empty_table(table, to_db):
    log(magenta('  empty') + ' ~> table ' + table.name)
    to_db.engine.execute(table.delete())


def update_sequences(to_db):
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
    from_database = db.engine.url.database
    to_database = to_db.engine.url.database
    if db.engine.dialect.name == 'postgresql':
        db.session.connection().connection.set_isolation_level(0)
        db.session.execute(
            '''
                CREATE DATABASE "%s" WITH TEMPLATE "%s";
            ''' %
            (
                to_database,
                from_database
            )
        )
        db.session.connection().connection.set_isolation_level(1)
    elif db.engine.dialect.name == 'mysql':
        # Horribly slow implementation.
        create_database(to_db.engine.url, to_database)
        for row in db.sesssion.execute('SHOW TABLES in %s;' % from_database):
            db.session.execute('''
                CREATE TABLE %s.%s LIKE %s.%s
            ''' % (
                to_database,
                row[0],
                from_database,
                row[0]
            ))
            db.session.execute('ALTER TABLE %s.%s DISABLE KEYS' % (
                to_database,
                row[0]
            ))
            db.session.execute('''
                INSERT INTO %s.%s SELECT * FROM %s.%s
            ''' % (
                to_database,
                row[0],
                from_database,
                row[0]
            ))
            db.session.execute('ALTER TABLE %s.%s ENABLE KEYS' % (
                to_database,
                row[0]
            ))
    else:
        raise NotSupportedDatabase()
