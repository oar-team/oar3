# -*- coding: utf-8 -*-

import click

from .utils import CommandReturns
from oar.lib.resource_handling import remove_resource

click.disable_unicode_literals_warning = True

@click.command()
@click.argument('resource', required=True,  type=int,)
def cli(resource):
    #import pdb; pdb.set_trace()
    resource_id = resource
    cmd_ret = CommandReturns(cli)
    if resource_id:
        error, error_msg = remove_resource(resource_id)
        cmd_ret.error(error_msg, error, error)
        cmd_ret.exit()
    else:
        cmd_ret.exit(1)
