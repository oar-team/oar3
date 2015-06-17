# -*- coding: utf-8 -*-
from __future__ import division, absolute_import, unicode_literals


from sqlalchemy import MetaData

from oar.lib.compat import to_unicode

from .helpers import yellow, blue, red

from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.autogenerate import compare_metadata


SUPPORTED_ALEMBIC_OPERATIONS = {
    'remove_column': 'drop column %s from table %s',
    # 'remove_index': 'drop index %s for columns (%s)',
    'add_table': 'create table %s',
    'add_column': 'add column %s to table %s',
    'modify_type': 'modify column %s.%s type to %s',
    'modify_nullable': 'modify column %s.%s by setting '
                       'nullable property to %s',
    # 'add_index': 'drop index',
}


def alembic_generate_diff(from_engine, to_engine):
    opts = {'compare_type': True}
    supported_operations = SUPPORTED_ALEMBIC_OPERATIONS.keys()
    mc = MigrationContext.configure(to_engine.connect(), opts=opts)
    from_metadata = MetaData()
    from_metadata.reflect(bind=from_engine)

    def all_diffs():
        for diff in compare_metadata(mc, from_metadata):
            if isinstance(diff[0], tuple):
                op_name = diff[0][0]
            else:
                op_name = diff[0]
            if op_name in supported_operations:
                yield op_name, diff
    key_sort = lambda x: supported_operations.index(x[0])
    return sorted(all_diffs(), key=key_sort)


def alembic_apply_diff(ctx, op, op_name, diff, tables=[]):
    tables_dict = dict(((table.name, table) for table in tables))
    supported_operations = SUPPORTED_ALEMBIC_OPERATIONS.keys()
    if op_name not in supported_operations:
        raise ValueError("Unsupported '%s' operation" % op_name)

    if op_name == "add_table":
        table_name = diff[1].name
        columns = [c.copy() for c in diff[1].columns]
        msg = SUPPORTED_ALEMBIC_OPERATIONS[op_name] % table_name
        op_callback = lambda: op.create_table(table_name, *columns)
    elif op_name in ('add_column', 'remove_column'):
        column = diff[3].copy()
        table_name = diff[2]
        if 'add' in op_name:
            op_callback = lambda: op.add_column(diff[2], column)
        else:
            op_callback = lambda: op.drop_column(diff[2], column.name)
        msg = SUPPORTED_ALEMBIC_OPERATIONS[op_name] % (column.name, table_name)
    elif op_name in ('remove_index', 'add_index'):
        index = diff[1]
        columns = [i for i in index.columns]
        table_name = index.table.name
        index_colums = ()
        for column in columns:
            index_colums += ("%s.%s" % (column.table.name, column.name),)
        if 'add' in op_name:
            args = (index.name, table_name, [c.name for c in columns],)
            kwargs = {'unique': index.unique}
            op_callback = lambda: op.create_index(*args, **kwargs)
        else:
            op_callback = lambda: op.drop_index(index.name)
        msg = SUPPORTED_ALEMBIC_OPERATIONS[op_name] \
            % (index.name, ",".join(index_colums))
    elif op_name in ('modify_type',):
        table_name = diff[0][2]
        column_name = diff[0][3]
        kwargs = diff[0][4]
        type_ = diff[0][6]

        def op_callback():
            try:
                op.alter_column(table_name, column_name, server_default=None)
                op.alter_column(table_name, column_name, type_=type_, **kwargs)
            except:
                # Some types cannot be casted
                if table_name in tables_dict:
                    table = tables_dict[table_name]
                    column = table.columns[column_name].copy()
                    op.drop_column(table_name, column_name)
                    op.add_column(table_name, column)
        msg = SUPPORTED_ALEMBIC_OPERATIONS[op_name] \
            % (table_name, column_name, type_)
    elif op_name in ('modify_nullable',):
        table_name = diff[0][2]
        column_name = diff[0][3]
        kwargs = diff[0][4]
        nullable = diff[0][6]

        def op_callback():
            op.alter_column(table_name, column_name,
                            nullable=nullable, **kwargs)
        msg = SUPPORTED_ALEMBIC_OPERATIONS[op_name] \
            % (table_name, column_name, nullable)
    try:
        if msg is not None:
            ctx.log("%s ~> %s" % (yellow('upgrade'), msg))
        op_callback()
    except Exception as ex:
        ctx.log(*red(to_unicode(ex)).splitlines(), prefix=(' ' * 9))


def alembic_sync_schema(ctx, from_engine, to_engine, tables=[]):
    # ctx.current_db.reflect()
    message = blue('compare') + ' ~> databases schemas'
    ctx.log(message + ' (in progress)')
    diffs = list(alembic_generate_diff(from_engine, to_engine))

    ctx.log("%s (%s)" % (message, blue("%s changes" % len(diffs))))

    mc = MigrationContext.configure(to_engine.connect())
    op = Operations(mc)

    for op_name, diff in diffs:
        alembic_apply_diff(ctx, op, op_name, diff, tables=tables)
