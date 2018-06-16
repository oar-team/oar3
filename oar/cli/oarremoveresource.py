# -*- coding: utf-8 -*-

import click

from .utils import CommandReturns
from oar.lib.resource_handling import remove_resource

click.disable_unicode_literals_warning = True

@click.command()
@click.argument('resource', nargs=-1, required=True,  type=int)
def cli(resource):
    """Usage: oarremoveresource resource_id(s)
    WARNING : this command removes all records in the database
    about "resource_id(s)".

    So you will loose this resource history and jobs executed on this one
    """
    resource_ids = resource
    cmd_ret = CommandReturns(cli)
    if resource_ids:
        for resource_id in resource_ids:
            error, error_msg = remove_resource(resource_id)
            cmd_ret.error(error_msg, error, error)
    cmd_ret.exit()
