# -*- coding: utf-8 -*-

import click
click.disable_unicode_literals_warning = True

def oarremoveresource():
    pass

@click.command()
@click.argument('resource', required=True)
def cli(resource, cli=True):
    pass
