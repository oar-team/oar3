# -*- coding: utf-8 -*-
from __future__ import division
import sys
import click

from sqlalchemy import func
from sqlalchemy.sql.expression import select
from sqlalchemy.engine.reflection import Inspector
from oar.lib import config, db, Database
from oar.utils import VERSION

from oar.lib.compat import iteritems, itervalues, reraise


magenta = lambda x: click.style("%s" % x, fg="magenta")
yellow = lambda x: click.style("%s" % x, fg="yellow")
green = lambda x: click.style("%s" % x, fg="green")
blue = lambda x: click.style("%s" % x, fg="blue")
red = lambda x: click.style("%s" % x, fg="red")


def log(*args, **kwargs):
    """Logs a message to stderr."""
    kwargs.setdefault("file", sys.stderr)
    prefix = kwargs.pop("prefix", "")
    for msg in args:
        click.echo(prefix + msg, **kwargs)


def sync(db_url, db_archive_url, chunk_size=1000):
    db_archive = Database(uri=db_archive_url)
    create_all_tables(db_archive)
    copy_tables(db_archive, chunk_size)
    update_sequences(db_archive)


def create_all_tables(db_archive):
    db.reflect()
    inspector = Inspector.from_engine(db_archive.engine)
    existing_tables = inspector.get_table_names()
    for table in db.metadata.sorted_tables:
        if table.name not in existing_tables:
            log(' %s ~> table %s', green('create'), table.name)
            table.create(bind=db_archive.engine, checkfirst=True)


def copy_tables(db_archive, chunk_size):
    for name, Model in iteritems(db.models):
        copy_model(Model, db_archive, chunk_size)
    # Tables without primary keys
    tables_with_pk = (model.__table__ for model in itervalues(db.models))
    all_tables = (table for table in itervalues(db.tables))
    for table in set(all_tables) - set(tables_with_pk):
        empty_table(table, db_archive)
        copy_table(table, db_archive, chunk_size)


def merge_model(Model, db_archive):
    log(' %s ~> table %s', magenta(' merge'), Model.__table__.name)
    query_result = db.query(Model)
    for r in query_result:
         db_archive.session.merge(r)
    db_archive.session.commit()


def copy_model(Model, db_archive, chunk_size):
    # prepare the connection
    to_connection = db_archive.engine.connect()
    # Get the max pk
    pk = Model.__mapper__.primary_key[0]
    max_pk_query = select([func.max(pk)])
    max_pk = to_connection.execute(max_pk_query).scalar()
    condition = None
    if max_pk is not None:
        condition = (pk > max_pk)
    table = Model.__table__
    copy_table(table, db_archive, chunk_size, select_condition=condition)


def copy_table(table, db_archive, chunk_size, select_condition=None):
    # prepare the connection
    to_connection = db_archive.engine.connect()
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
    result = from_connection.execute(select_query)

    message = yellow('\r   copy') + ' ~> table %s (%s)'
    log(message % (table.name, blue("0/%s" % total_lenght)), nl=False)
    if total_lenght > 0:
        progress = 0
        transaction = to_connection.begin()
        while True:
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


def empty_table(table, db_archive):
    log(red(' delete') + ' ~> table ' + table.name)
    db_archive.engine.execute(table.delete())


def update_sequences(db_archive):
    engine = db_archive.engine
    def get_sequences_values():
        for model in itervalues(db.models):
            for pk in model.__mapper__.primary_key:
                if not pk.autoincrement:
                    continue
                sequence_name = "%s_%s_seq" % (pk.table.name, pk.name)
                if engine.dialect.has_sequence(engine, sequence_name):
                    yield sequence_name, pk.name, pk.table.name

    if db_archive.engine.url.get_dialect().name == "postgresql":
        for sequence_name, pk_name, table_name in get_sequences_values():
            log(green('\r    fix') + ' ~> sequence %s' % sequence_name)
            query = "select setval('%s', max(%s)) from %s"
            try:
                engine.execute(query % (sequence_name, pk_name, table_name))
            except Exception as ex:
                log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))
        db_archive.commit()


def get_default_database_url():
    try:
        return config.get_sqlalchemy_uri()
    except:
        pass


CONTEXT_SETTINGS = dict(auto_envvar_prefix='oar',
                        help_option_names=['-h', '--help'])

DATABASE_PROMPT = """Please enter the url for your database.

For example:
PostgreSQL: postgresql://scott:tiger@localhost/foo
MySQL: mysql://scott:tiger@localhost/bar

OAR database URL"""

@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=VERSION)
@click.option('--sql', is_flag=True, default=False,
              help='Only dump SQL statements to STDOUT (offline mode)')
@click.option('--db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(),
              help='the url for your OAR database.')
@click.option('--db-archive-url', prompt="OAR archive database URL",
              help='the url for your archive database.')
@click.option('--debug', is_flag=True, default=False,
              help="Enable Debug.")
def cli(debug, db_archive_url, db_url, sql):
    """Archive OAR database."""
    config._sqlalchemy_uri = db_url
    try:
        sync(db_url, db_archive_url, chunk_size=10000)
        log("up-to-date.")
    except Exception as e:
        if not debug:
            sys.stderr.write(u"\nError: %s\n" % e)
            sys.exit(1)
        else:
            exc_type, exc_value, tb = sys.exc_info()
            reraise(exc_type, exc_value, tb.tb_next)


def main(args=sys.argv[1:]):
    cli(args)
