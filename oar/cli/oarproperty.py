# -*- coding: utf-8 -*-
#
#  Manage OAR resource properties.
#
# This script aims at managing the node properties (list, add, delete).
# To set the properties values, use oarnodesettings.
#
# To use the quiet mode, just do something like:
#   echo -e "mysqlroot\nmysqlpassword\n" | oar_property.pl -q -l
import click
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import VARCHAR, Column, Integer, String
from sqlalchemy.orm import scoped_session, sessionmaker

from oar import VERSION
from oar.lib.globals import init_oar
from oar.lib.models import JobResourceDescription, Model, Resource, ResourceLog
from oar.lib.tools import check_resource_property

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


def check_property_name(cmd_ret, prop_name, quiet=False):
    if not prop_name.isalpha():
        if not quiet:
            cmd_ret.error("'{}' is not a valid property name".format(prop_name))
        return True
    else:
        if check_resource_property(prop_name):
            if not quiet:
                cmd_ret.warning(
                    "'{}' is a OAR system property and may not be altered".format(
                        prop_name
                    ),
                    1,
                )
            return True
    return False


def get_op(engine):
    ctx = MigrationContext.configure(engine)
    return Operations(ctx)


def oarproperty(
    session,
    engine,
    config,
    prop_list,
    show_type,
    add,
    varchar,
    delete,
    rename,
    quiet,
    version,
):
    cmd_ret = CommandReturns()

    Model.metadata.reflect(bind=engine)
    # print(vars(Model.metadata.tables["resources"]))
    # it's mainly use Operations from Alembic through db.op
    # import pdb; pdb.set_trace()
    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    resources = Resource.__tablename__
    # get properties from tables

    # Reflect to load all the columns from the database
    # (including the columns not in the class Resource)
    # session.get_bind().reflect()
    columns = Model.metadata.tables["resources"].columns
    properties = [column.name for column in columns]

    if prop_list:
        for column in properties:
            if not check_resource_property(column):
                print(column)

    op = get_op(engine.connect())
    if delete:
        for prop_todelete in delete:
            if check_property_name(cmd_ret, prop_todelete):
                return cmd_ret

            op.drop_column(resources, prop_todelete)
            if not quiet:
                cmd_ret.print_("Deleled property: {}".format(prop_todelete))

    if add:
        for prop_toadd in add:
            skip = False
            prop_toadd = prop_toadd.lstrip()
            if check_property_name(cmd_ret, prop_toadd):
                return cmd_ret
            if prop_toadd in properties:
                if varchar and (type(columns[prop_toadd]) != VARCHAR):
                    cmd_ret.error(
                        "Property '{}' already exists but with type mismatch.".format(
                            prop_toadd
                        ),
                        2,
                        2,
                    )
                    skip = True
                else:
                    if not quiet:
                        cmd_ret.print_(
                            "Property '{}' already exists.".format(prop_toadd)
                        )
                    skip = True
            if not skip:
                kw = {"nullable": True}
                if varchar:
                    op.add_column(resources, Column(prop_toadd, String(255), **kw))
                else:
                    op.add_column(resources, Column(prop_toadd, Integer, **kw))
                if not quiet:
                    cmd_ret.print_("Added property: {}".format(prop_toadd))

    if rename:
        for prop_torename in rename:
            old_prop, new_prop = prop_torename.split(",")
            if check_property_name(cmd_ret, old_prop):
                return cmd_ret
            if check_property_name(cmd_ret, new_prop):
                return cmd_ret
            if old_prop not in columns:
                cmd_ret.error(f"Cannot rename unexistent property: '{old_prop}'")
                return cmd_ret
            if new_prop in columns:
                cmd_ret.error(
                    f"cannot rename '{old_prop}' into '{new_prop}'. Property '{new_prop}' already exists."
                )
                return cmd_ret

            op.alter_column(resources, old_prop, new_column_name=new_prop)

            session.query(ResourceLog).filter(ResourceLog.attribute == old_prop).update(
                {ResourceLog.attribute: new_prop}, synchronize_session=False
            )

            session.query(JobResourceDescription).filter(
                JobResourceDescription.resource_type == old_prop
            ).update(
                {JobResourceDescription.resource_type: new_prop},
                synchronize_session=False,
            )
            session.commit()
            if not quiet:
                cmd_ret.print_("Rename property {} into {}".format(old_prop, new_prop))

    return cmd_ret


@click.command()
@click.option("-l", "--list", is_flag=True, help="List the properties.")
@click.option("-t", "--type", is_flag=True, help="Show the types of the properties.")
@click.option(
    "-a",
    "--add",
    type=click.STRING,
    multiple=True,
    help="Add a property (integer by default).",
)
@click.option("-c", "--varchar", is_flag=True, help="Property is a character string.")
@click.option(
    "-d", "--delete", type=click.STRING, multiple=True, help="Delete a property."
)
@click.option(
    "-r",
    "--rename",
    type=click.STRING,
    multiple=True,
    help='Rename a property from OLD to NEW name ("OLD,NEW").',
)
@click.option("-q", "--quiet", is_flag=True, help="Quiet mode (no extra output).")
@click.option("-V", "--version", is_flag=True, help="Show OAR version.")
def cli(list, type, add, varchar, delete, rename, quiet, version):
    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
        engine = session.get_bind()
    else:
        config, engine, log = init_oar()
        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    prop_list = list
    show_type = type
    cmd_ret = oarproperty(
        session,
        engine,
        config,
        prop_list,
        show_type,
        add,
        varchar,
        delete,
        rename,
        quiet,
        version,
    )
    cmd_ret.exit()
