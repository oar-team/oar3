# -*- coding: utf-8 -*-
import sys
import click

from sqlalchemy.sql.expression import select
from sqlalchemy import func
from oar.lib import config, db, Database
from oar.utils import VERSION
from oar.lib.models import *



blue = lambda x: click.style("%s" % x, fg="blue")
green = lambda x: click.style("%s" % x, fg="green")
red = lambda x: click.style("%s" % x, fg="red")


def log(msg, *args):
    """Logs a message to stderr."""
    if args:
        msg %= args
    click.echo(msg, file=sys.stderr)


def sync(db_url, db_archive_url, chunk_size=1000):
    db_archive = Database(uri=db_archive_url)
    create_all_tables(db_archive)
    sync_data(db_archive, chunk_size)


def create_all_tables(db_archive):
    db.reflect()
    inspector = Inspector.from_engine(db_archive.engine)
    existing_tables = inspector.get_table_names()
    for table in db.metadata.sorted_tables:
        if table.name not in existing_tables:
            log(' %s ~> table %s', green('create'), table.name)
            table.create(bind=db_archive.engine, checkfirst=True)


def sync_data(db_archive, chunk_size):

    def logging(lenght, table_name):
        message = '   %s | Copying %s record(s) from %s'
        log(message, green('data'), blue(lenght), table_name)

    for table in db.metadata.sorted_tables:
        lenght = db.session.execute(select([func.count()])\
                           .select_from(table))\
                           .scalar()
        insert_query = table.insert()
        select_query = select([table])
        result = db.session.execute(select_query)
        logging(lenght, table.name)
        while True:
            rows = result.fetchmany(chunk_size)
            if len(rows) == 0:
                break
            db_archive.session.execute(insert_query, rows)
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
PostgreSQL: postgresql://scott:tiger@localhost/mydatabase
MySQL: mysql://scott:tiger@localhost/foo

OAR database URL"""


@click.command(context_settings=CONTEXT_SETTINGS)
@click.version_option(version=VERSION)
@click.option('--sql', default=1, help='Dump the SQL (offline mode)')
@click.option('--db-url', prompt=DATABASE_PROMPT,
              default=get_default_database_url(),
              help='the url for your OAR database.')
@click.option('--db-archive-url', prompt="OAR archive database URL",
              help='the url for your archive database.',
              default="postgresql://oar:oar@server:5432/oar_archive")
def cli(db_url, db_archive_url, sql):
    """Archive OAR database."""
    config._sqlalchemy_uri = db_url
    sync(db_url, db_archive_url, chunk_size=1000)

def main(args=sys.argv[1:]):
    try:
        cli(args)
    except Exception as e:
        sys.stderr.write(u"\nError: %s\n" % e)
        sys.exit(1)
