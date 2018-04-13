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
from oar.lib import (db, config)
from oar.lib.tools import check_resource_property
from sqlalchemy import VARCHAR
from .utils import CommandReturns
import click

click.disable_unicode_literals_warning = True

def check_property_name(cmd_ret, prop_name):
    if not prop_name.isalpha():
        cmd_ret.error("'{}' is not a valid property name".format(prop_name)) 
        return True
    else:
        if check_resource_property(prop_name):
            cmd_ret.warning("'{}' is a OAR system property and may not be altered".format(prop_name))
            return True
    return False

def oarproperty(prop_list, show_type, add, varchar, delete, rename, quiet, version):
    cmd_ret = CommandReturns()
    # get properties from tables
    properties = db['resources'].columns
    
    if prop_list:
        # TODO
        pass
        # properties = list_resource_properties_fields()

    if delete:
        for prop_todelete in delete:
            # TODO
            pass
    
    if add:
        for prop_toadd in add:
            prop_toadd = prop_toadd.lstrip()
            if check_property_name(cmd_ret, prop_toadd):
                return cmd_ret
            if prop_toadd in properties:
                if varchar and (type(db['resources'].columns[prop_toadd]) != VARCHAR):
                    cmd_ret.error("Property '{}' already exists but with type mismatch."\
                                  .format(prop_toadd),2,2) 
                    return cmd_ret
                else:
                    if not quiet:
                        cmd_ret.print_("Property '{}' already exists.".format(prop_toadd))
                    next
            kw = {"nullable": True}
            if varchar:
                db.op.add_column('resources', db.Column(prop_toadd, db.String(255), **kw))
            else:
                db.op.add_column('resources', db.Column(prop_toadd, db.Integer, **kw))
            if not quiet:
               cmd_ret.print_("Added property: {}".format(prop_toadd))

    if prop_torename:
        for prop_torename in rename:
            # TODO
            pass
               
    db.close()
    return cmd_ret

@click.command()
@click.option('-l', '--list', is_flag=True, help='List the properties.')
@click.option('-t', '--type', is_flag=True, help='Show the types of the properties.')
@click.option('-a', '--add', type=click.STRING, help='Add a property (integer).')
@click.option('-c', '--varchar', is_flag=True, help='Property is a character string.')
@click.option('-d', '--delete', type=click.STRING, help='Delete a property.')
@click.option('-r', '--rename', type=click.STRING, help='Rename a property from OLD to NEW name.')
@click.option('-q', '--quiet', is_flag=True, help='Quiet mode (no extra output).')
@click.option('-V', '--version', is_flag=True, help='Show OAR version.')
def cli(list, type, add, varchar, delete, rename, quiet, version):
    prop_list = list   
    show_type = type
    if isinstance(add, str): add = [add]
    if isinstance(delete, str): delete = [delete]
    if isinstance(rename, str): rename = [rename]
    cmd_ret = oarproperty(prop_list, show_type, add, varchar, delete, rename, quiet, version)           
    cmd_ret.exit()
