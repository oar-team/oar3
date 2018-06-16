# -*- coding: utf-8 -*-
#
#  Manage OAR resource properties.
#
# This script aims at managing the node properties (list, add, delete).
# To set the properties values, use oarnodesettings.
#
# To use the quiet mode, just do something like:
#   echo -e "mysqlroot\nmysqlpassword\n" | oar_property.pl -q -l
from oar import VERSION
from oar.lib import (db, config, Resource)
from oar.lib.tools import check_resource_property
from sqlalchemy import VARCHAR
from .utils import CommandReturns
import click

click.disable_unicode_literals_warning = True

def check_property_name(cmd_ret, prop_name, quiet=False):
    if not prop_name.isalpha():
        if not quiet:
            cmd_ret.error("'{}' is not a valid property name".format(prop_name)) 
        return True
    else:
        if check_resource_property(prop_name):
            if not quiet:
                cmd_ret.warning("'{}' is a OAR system property and may not be altered".format(prop_name))
            return True
    return False

def oarproperty(prop_list, show_type, add, varchar, delete, rename, quiet, version):
    cmd_ret = CommandReturns()

    # it's mainly use Operations from Alembic through db.op

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    resources = Resource.__tablename__
    # get properties from tables
    properties = db[resources].columns
    #import pdb; pdb.set_trace()
    if prop_list:
        for column in properties:
            if not check_resource_property(column.name):
                print(column.name)

    if delete:
        for prop_todelete in delete:
            # TODO
            pass
        #drop_column(table_name, column_name, schema=None, **kw)¶
        
    if add:
        for prop_toadd in add:
            prop_toadd = prop_toadd.lstrip()
            if check_property_name(cmd_ret, prop_toadd):
                return cmd_ret
            if prop_toadd in properties:
                if varchar and (type(properties[prop_toadd]) != VARCHAR):
                    cmd_ret.error("Property '{}' already exists but with type mismatch."\
                                  .format(prop_toadd),2,2) 
                    return cmd_ret
                else:
                    if not quiet:
                        cmd_ret.print_("Property '{}' already exists.".format(prop_toadd))
                    next
            kw = {"nullable": True}
            if varchar:
                db.op.add_column(resources, db.Column(prop_toadd, db.String(255), **kw))
            else:
                db.op.add_column(resources, db.Column(prop_toadd, db.Integer, **kw))
            if not quiet:
               cmd_ret.print_("Added property: {}".format(prop_toadd))

    if rename:
        for prop_torename in rename:
            # TODO 
            pass
        #alter_column(table_name, column_name, nullable=None, server_default=False, new_column_name=N
    db.close()
    return cmd_ret

@click.command()
@click.option('-l', '--list', is_flag=True, help='List the properties.')
@click.option('-t', '--type', is_flag=True, help='Show the types of the properties.')
@click.option('-a', '--add', type=click.STRING, multiple=True,
              help='Add a property (integer by default).')
@click.option('-c', '--varchar', is_flag=True, help='Property is a character string.')
@click.option('-d', '--delete', type=click.STRING, multiple=True,
              help='Delete a property.')
@click.option('-r', '--rename', type=click.STRING, multiple=True,
              help='Rename a property from OLD to NEW name ("OLD,NEW").')
@click.option('-q', '--quiet', is_flag=True, help='Quiet mode (no extra output).')
@click.option('-V', '--version', is_flag=True, help='Show OAR version.')
def cli(list, type, add, varchar, delete, rename, quiet, version):
    prop_list = list   
    show_type = type
    cmd_ret = oarproperty(prop_list, show_type, add, varchar, delete, rename, quiet, version)           
    cmd_ret.exit()
