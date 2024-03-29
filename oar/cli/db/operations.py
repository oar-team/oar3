# -*- coding: utf-8 -*-
import re
import sys
from copy import copy
from functools import partial, reduce
from itertools import chain

from sqlalchemy import MetaData, Table, and_
from sqlalchemy import asc as order_by_func
from sqlalchemy import create_engine, func, not_
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.expression import select
from sqlalchemy.types import Integer
from sqlalchemy_utils.functions import (
    create_database,
    database_exists,
    drop_database,
    render_statement,
)

import oar.lib.tools as tools
from oar.lib import models
from oar.lib.models import JOBS_TABLES, MOLDABLES_JOBS_TABLES, RESOURCES_TABLES
from oar.lib.utils import reraise, to_unicode

from .alembic import alembic_sync_schema
from .helpers import blue, green, magenta, red, yellow


def archive_db(ctx):
    from_engine = create_engine(ctx.current_db.engine.url)
    to_engine = create_engine(ctx.archive_db.engine.url)

    ignored_tables = [
        "accounting",
        "gantt_jobs_predictions",
        "gantt_jobs_predictions_log",
        "gantt_jobs_predictions_visu",
        "gantt_jobs_resources",
        "gantt_jobs_resources_log",
        "gantt_jobs_resources_visu",
    ]
    if is_local_database(from_engine, to_engine) and from_engine.dialect.name in (
        "postgresql",
        "mysql",
    ):
        clone_db(ctx, ignored_tables=ignored_tables)
        tables = list(sync_schema(ctx, None, from_engine, to_engine))
        # copy data
        sync_tables(
            ctx,
            sorted(tables, key=lambda x: x.name),
            ctx.current_db,
            ctx.archive_db,
            delete=True,
            ignored_tables=ignored_tables,
        )
    else:
        # Create databases
        create_database_if_not_exists(ctx, to_engine)

        if from_engine.dialect.name != to_engine.dialect.name:
            # Collect all tables from our models
            tables = [table for name, table in models.all_tables()]
            # Create missing tables
            tables = list(sync_schema(ctx, tables, from_engine, to_engine))
            # Upgrade schema
            alembic_sync_schema(ctx, from_engine, to_engine, tables=tables)
        else:
            tables = None
            # Create missing tables
            tables = list(sync_schema(ctx, tables, from_engine, to_engine))

        tables = sorted(tables, key=lambda x: x.name)

        sync_tables(
            ctx,
            sorted(tables, key=lambda x: x.name),
            ctx.current_db,
            ctx.archive_db,
            delete=True,
            ignored_tables=ignored_tables,
        )
    fix_sequences(ctx, to_engine, tables)


def migrate_db(ctx):
    from_engine = create_engine(ctx.current_db.engine.url)
    to_engine = create_engine(ctx.new_db.engine.url)
    # Create databases
    if not ctx.data_only:
        create_database_if_not_exists(ctx, to_engine)

    tables = [table for name, table in models.all_tables()]

    if not ctx.data_only:
        # Create
        tables = list(sync_schema(ctx, tables, from_engine, to_engine))
        alembic_sync_schema(ctx, from_engine, to_engine, tables=tables)

    if not ctx.schema_only:
        new_tables = []
        for table in tables:
            new_tables.append(reflect_table(to_engine, table.name))

        tables_to_empty = ("admission_rules",)

        for table in new_tables:
            with to_engine.engine.connect() as to_conn:
                if table.name in tables_to_empty:
                    delete_from_table(ctx, table, to_conn)

        sync_tables(ctx, new_tables, ctx.current_db, ctx.new_db)

        fix_sequences(ctx, to_engine, new_tables)


jobs_table = [list(d.keys())[0] for d in JOBS_TABLES]
moldable_jobs_tables = [list(d.keys())[0] for d in MOLDABLES_JOBS_TABLES]
resources_tables = [list(d.keys())[0] for d in RESOURCES_TABLES]


def create_db(ctx):
    ctx.log("Creating the database user...\n")
    db_user = ctx.conf["DB_BASE_LOGIN"]
    db_pass = ctx.conf["DB_BASE_PASSWD"]
    db_user_ro = ctx.conf["DB_BASE_LOGIN_RO"]
    db_pass_ro = ctx.conf["DB_BASE_PASSWD_RO"]
    db_name = ctx.conf["DB_BASE_NAME"]

    #
    # TODO from setup/database/oar-database.in
    # return system("echo \"$query\" | su - postgres -c \"psql $db\"");
    #

    pgsql = ' |su - postgres -c "psql"'
    pgsql_db = " |su - postgres -c \"psql '{}'\"".format(db_name)

    tools.call(
        "echo \"CREATE ROLE {} LOGIN PASSWORD '{}';\"{}".format(
            db_user, db_pass, pgsql
        ),
        shell=True,
    )
    tools.call(
        "echo \"CREATE ROLE {} LOGIN PASSWORD '{}';\"{}".format(
            db_user_ro, db_pass_ro, pgsql
        ),
        shell=True,
    )

    ctx.log("Creating the database...\n")
    tools.call(
        'echo "CREATE DATABASE {} OWNER {};"{}'.format(db_name, db_user, pgsql),
        shell=True,
    )

    tools.call(
        'echo "REVOKE CREATE ON SCHEMA public FROM PUBLIC;"{}'.format(pgsql_db),
        shell=True,
    )

    tools.call(
        'echo "GRANT CREATE ON SCHEMA public TO {};"{}'.format(db_user, pgsql_db),
        shell=True,
    )

    tools.call(
        'echo "GRANT ALL PRIVILEGES ON DATABASE {} TO {}"{}'.format(
            db_name, db_user, pgsql_db
        ),
        shell=True,
    )

    engine = create_engine(ctx.current_db.engine.url)
    # Create database
    if not create_database_if_not_exists(ctx, engine):
        ctx.log("\nNothing to do.")


def drop_db(ctx):
    engine = create_engine(ctx.current_db.engine.url)
    # Drop database
    if not drop_database_if_exists(ctx, engine):
        ctx.log("\nNothing to do.")


def upgrade_db(ctx):
    # engine = create_engine(ctx.current_db.engine.url) # TODO
    ctx.log(red("NOT YET IMPLEMENTED"))
    sys.exit(1)


def reset_db(ctx):
    # engine = create_engine(ctx.current_db.engine.url) # TODO
    ctx.log(red("NOT YET IMPLEMENTED"))
    sys.exit(1)


def check_db(ctx):
    try:
        # engine = create_engine(ctx.current_db.engine.url) # TODO
        ctx.log(" %s ~> %s" % (green("check"), "Database connection is operational"))
        sys.exit(0)
    except Exception as ex:
        ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(" " * 9))
        sys.exit(1)


def get_table_columns(tables, table_name):
    return [d[table_name] for d in tables if table_name in d.keys()]


get_jobs_columns = partial(get_table_columns, JOBS_TABLES)
get_moldables_columns = partial(get_table_columns, MOLDABLES_JOBS_TABLES)
get_resources_columns = partial(get_table_columns, RESOURCES_TABLES)


def get_jobs_sync_criterion(ctx, table):
    # prepare query
    criterion = []
    if hasattr(ctx, "max_job_to_sync"):
        if table.name in jobs_table:
            for column_name in get_jobs_columns(table.name):
                column = table.c.get(column_name)
                criterion.append(column < ctx.max_job_to_sync)
    if hasattr(ctx, "max_moldable_job_to_sync"):
        if table.name in moldable_jobs_tables:
            for column_name in get_moldables_columns(table.name):
                column = table.c.get(column_name)
                criterion.append(column < ctx.max_moldable_job_to_sync)
    return criterion


def get_resources_purge_criterion(ctx, table):
    # prepare query
    criterion = []
    if hasattr(ctx, "resources_to_purge"):
        if table.name in resources_tables:
            for column_name in get_resources_columns(table.name):
                column = table.c.get(column_name)
                if ctx.resources_to_purge:
                    criterion.append(column.in_(ctx.resources_to_purge))
    return criterion


def get_primary_keys(table):
    # Deterministic order
    # Order by name and Integer type first
    pks = sorted((c for c in table.c if c.primary_key), key=lambda c: c.name)
    return sorted(pks, key=lambda x: not isinstance(x.type, Integer))


def get_first_primary_key(table):
    for pk in get_primary_keys(table):
        return pk


def create_database_if_not_exists(ctx, engine):
    create_database(engine.url)
    # if not database_exists(engine.url):
    #     #NOTE: NOT USED (DB create before)
    #     ctx.log(f"{green(' create')} ~> new database `{engine.url}`")
    #     create_database(engine.url)
    #     return True
    # return False


def drop_database_if_exists(ctx, engine):
    if database_exists(engine.url):
        ctx.log(f"{green(' drop')} ~> drop database `{engine.url}`")
        drop_database(engine.url)
        return True
    return False


def generic_mapper(table):
    class GenericMapper(declarative_base()):
        __table__ = table

    return GenericMapper


def is_local_database(from_engine, to_engine):
    url = copy(to_engine.url)
    url.database = from_engine.engine.url.database
    return url == from_engine.engine.url


class NotSupportedDatabase(Exception):
    pass


def reflect_table(db, table_name):
    metadata = MetaData(db.engine)
    return Table(table_name, metadata, autoload=True)


def clone_db(ctx, ignored_tables=()):
    message = " ~> `%s` to `%s` database" % (ctx.current_db_name, ctx.archive_db_name)
    ctx.log(green("  clone") + message)
    if ctx.current_db.dialect == "postgresql":
        ctx.current_db.session.connection().connection.set_isolation_level(0)
        ctx.current_db.session.execute(
            'CREATE DATABASE "%s" WITH TEMPLATE "%s";'
            % (ctx.archive_db_name, ctx.current_db_name)
        )
        ctx.current_db.session.connection().connection.set_isolation_level(1)
    elif ctx.current_db.dialect == "mysql":
        # Horribly slow implementation.
        create_database_if_not_exists(ctx, ctx.archive_db.engine)
        show_tables_query = "SHOW TABLES in %s;" % ctx.current_db_name
        for row in ctx.current_db.session.execute(show_tables_query):
            name = row[0]
            ctx.current_db.session.execute(
                "CREATE TABLE %s.%s LIKE %s.%s"
                % (ctx.archive_db_name, name, ctx.current_db_name, name)
            )
            if name in ignored_tables:
                ctx.log(yellow(" ignore") + " ~> table %s" % name)
                continue
            ctx.current_db.session.execute(
                "ALTER TABLE %s.%s DISABLE KEYS" % (ctx.archive_db_name, name)
            )
            table = reflect_table(ctx.current_db.engine, name)
            criterion = get_jobs_sync_criterion(ctx, table)
            query = select([table])
            if criterion:
                query = query.where(reduce(and_, criterion))
            raw_sql = render_statement(query, ctx.current_db.engine)
            raw_sql = raw_sql.replace(";", "").replace("\n", "")
            matcher = re.compile(r"SELECT(.*?)FROM", re.IGNORECASE | re.DOTALL)
            result = matcher.search(raw_sql)
            if result:
                raw_sql = raw_sql.replace(result.group(1), " * ")
            ctx.current_db.session.execute(
                "INSERT INTO %s.%s (%s)" % (ctx.archive_db_name, name, raw_sql)
            )
            ctx.current_db.session.execute(
                "ALTER TABLE %s.%s ENABLE KEYS" % (ctx.archive_db_name, name)
            )
    else:
        raise NotSupportedDatabase()


def sync_schema(ctx, tables, from_engine, to_engine):
    inspector = Inspector.from_engine(to_engine)
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
            ctx.log(" %s ~> table %s" % (green("create"), table.name))
            try:
                table.create(bind=to_engine, checkfirst=True)
            except Exception as ex:
                ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(" " * 9))
    metadata = MetaData(from_engine)
    for table in tables:
        # Make sure we have the good version of the table
        yield Table(table.name, metadata, autoload=True)


def sync_tables(ctx, tables, from_db, to_db, ignored_tables=(), delete=False):
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
                        copy_table(ctx, table, from_conn, to_conn, pk)
                    else:
                        merge_table(ctx, table, from_db, to_db)
                else:
                    delete_from_table(ctx, table, to_conn)
                    copy_table(ctx, table, from_conn, to_conn)

    for table in tables:
        with from_db.engine.connect() as from_conn:
            with to_db.engine.connect() as to_conn:
                do_sync(table, from_conn, to_conn)


def copy_table(ctx, table, from_conn, to_conn, pk=None):
    use_pg_copy = False
    if hasattr(ctx, "pg_copy") and ctx.pg_copy:
        if hasattr(to_conn.dialect, "psycopg2_version"):
            use_pg_copy = True

    insert_query = table.insert()
    select_query = select([table])
    count_query = select([func.count()]).select_from(table)

    if pk is not None:
        min_pk = to_conn.execute(select([func.max(pk)])).scalar()
        if min_pk is not None:
            count_query = count_query.where(pk > min_pk)

    select_query = select_query.order_by(
        *(order_by_func(pk) for pk in get_primary_keys(table))
    )

    total_lenght = from_conn.execute(count_query).scalar()
    if total_lenght == 0:
        return

    select_query = select_query.execution_options(stream_results=True)

    def log(progress):
        percentage = blue("%s/%s" % (progress, total_lenght))
        message = yellow("\r   copy") + " ~> table %s (%s)"
        ctx.log(message % (table.name, percentage), nl=False)

    def fetch_stream():
        def gen_pagination_with_pk(chunk):
            max_pk_query = select([func.max(pk)])
            min_pk_query = select([func.min(pk)])

            max_pk = from_conn.execute(max_pk_query).scalar() or 0
            min_pk = from_conn.execute(min_pk_query).scalar() or 0
            min_pk = to_conn.execute(max_pk_query).scalar() or (min_pk - 1)

            left_seq = range(min_pk + 1, max_pk, chunk)
            right_seq = range(min_pk + chunk, max_pk + chunk, chunk)
            for min_id, max_id in zip(left_seq, right_seq):
                yield select_query.where(pk.between(min_id, max_id))

        if pk is not None:
            queries = gen_pagination_with_pk(ctx.chunk)
        else:
            queries = [select_query]

        progress = 0
        for query in queries:
            page = 0
            while True:
                q = query.offset(page * ctx.chunk).limit(ctx.chunk)
                rows = from_conn.execute(q)
                if rows.rowcount == 0:
                    break
                progress = rows.rowcount + progress
                if progress > total_lenght:
                    progress = total_lenght
                log(progress)
                yield (i for i in rows)
                page = page + 1
        ctx.log("")

    if not use_pg_copy:
        for rows in fetch_stream():
            to_conn.execute(insert_query, list(rows))
    else:
        from oar.lib.psycopg2 import pg_bulk_insert

        columns = None
        for rows in fetch_stream():
            if columns is None:
                first = next(rows, None)
                columns = ["%s" % k for k in first.keys()]
                rows = chain((first,), rows)
            try:
                with to_conn.begin():
                    cursor = to_conn.connection.cursor()
                    pg_bulk_insert(
                        cursor, table, rows, columns, binary=ctx.pg_copy_binary
                    )
            except Exception:
                exc_type, exc_value, tb = sys.exc_info()
                reraise(exc_type, exc_value, tb.tb_next)


def merge_table(ctx, table, from_db, to_db):
    # Very slow !!
    ctx.log(" %s ~> table %s" % (magenta(" merge"), table.name))
    model = generic_mapper(table)
    columns = table.columns.keys()
    for record in from_db.query(table).all():
        data = dict([(str(column), getattr(record, column)) for column in columns])
        to_db.session.merge(model(**data))
    to_db.session.commit()


def delete_from_table(ctx, table, raw_conn, criterion=None, message=None):
    if criterion is None:
        criterion = []
    if message:
        ctx.log(message)
    delete_query = table.delete()
    if criterion:
        delete_query = delete_query.where(reduce(and_, criterion))
    ctx.log(magenta(" delete") + " ~> table %s (in progress)" % table.name, nl=False)
    count = raw_conn.execute(delete_query).rowcount
    ctx.log(
        magenta("\r\033[2K delete") + " ~> table %s (%s)" % (table.name, blue(count))
    )
    return count


def delete_orphan(ctx, p_table, p_key, f_table, f_key, raw_conn, message=None):
    if message:
        ctx.log(message)
    dialect = raw_conn.engine.dialect.name
    if dialect == "mysql":
        raw_query = (
            "DELETE a FROM {f_table} a\n"
            "LEFT JOIN {p_table} b\n"
            "ON a.{f_key} = b.{p_key}\n"
            "WHERE b.{p_key} IS NULL".format(**locals())
        )
    elif dialect == "postgresql":
        raw_query = (
            "DELETE FROM {f_table} a\n"
            "USING {p_table} b\n"
            "WHERE a.{f_key} = b.{p_key}\n"
            "AND b.{p_key} IS NULL".format(**locals())
        )
    else:
        raw_query = (
            "DELETE FROM {f_table}\n"
            "WHERE {f_key} NOT IN "
            "(SELECT {p_key} from {p_table})".format(**locals())
        )
    ctx.log(magenta(" delete") + " ~> table %s (in progress)" % f_table, nl=False)
    count = raw_conn.execute(raw_query).rowcount
    ctx.log(magenta("\r\033[2K delete") + " ~> table %s (%s)" % (f_table, blue(count)))
    return count


def fix_sequences(ctx, to_engine=None, tables=None):
    if tables is None:
        tables = []
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
        ctx.log(green("\r    fix") + " ~> sequence %s" % sequence_name)
        query = "select setval('%s', max(%s)) from %s"
        try:
            to_engine.execute(query % (sequence_name, pk_name, table_name))
        except Exception as ex:
            ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(" " * 9))


def purge_db(ctx):
    # prepare the connection
    db = ctx.current_db
    raw_conn = db.engine.connect()
    inspector = Inspector.from_engine(db.engine)
    tables = [reflect_table(db.engine, name) for name in inspector.get_table_names()]
    count = 0
    rv = None
    message = "Purge old resources from database :"
    for table in tables:
        if table.name in resources_tables:
            criterion = get_resources_purge_criterion(ctx, table)
            if criterion:
                rv = delete_from_table(ctx, table, raw_conn, criterion, message)
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
    rv = delete_orphan(
        ctx,
        "event_logs",
        "event_id",
        "event_log_hostnames",
        "event_id",
        raw_conn,
        message,
    )
    count += rv
    message = "Purge orphan resources descriptions from database :"
    rv = delete_orphan(
        ctx,
        "job_resource_groups",
        "res_group_id",
        "job_resource_descriptions",
        "res_job_group_id",
        raw_conn,
        message,
    )
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
        infos[table_name] = {
            "current_db_size": size,
            "archive_db_size": 0,
            "diff": size,
        }
    if database_exists(ctx.archive_db.engine.url):
        for table_name, size in count_all(ctx.archive_db):
            infos[table_name]["archive_db_size"] = size
            diff = infos[table_name]["current_db_size"] - size
            infos[table_name]["diff"] = diff

    headers = ["Table", "Current DB size", "Archive DB size", "Diff"]
    rows = [
        (
            k,
            to_unicode(infos[k]["current_db_size"]),
            to_unicode(infos[k]["archive_db_size"]),
            to_unicode(infos[k]["diff"]),
        )
        for k in infos.keys()
    ]
    return sorted(rows, key=lambda x: x[0]), headers
