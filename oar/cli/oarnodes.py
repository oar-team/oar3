# -*- coding: utf-8 -*-
"""oarnodes - print OAR node properties
 EXAMPLES:
 oarnodes -l
   => returns the complete list without information  - status = 0
 oarnodes -s
   => returns only the state of nodes - status = 0
 oarnodes -h|--help
   => returns a help message - status = 0
 oarnodes host1 [.. hostn]
   => returns the information for hostX - status is 0 for every host known - 1 otherwise
"""
import click
click.disable_unicode_literals_warning = True


def oarnodes(resource, state, liste, events, sql, json, version, cli):

    config.setdefault_config(DEFAULT_CONFIG)

    cmd_ret = CommandReturns(cli)      

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret
    
    if sql:
        pass

    
@click.command()

@click.option('-r', '--resource', type=int,
              help='show the properties of the resource whose id is given as parameter')
@click.option('--sql', type=click.STRING,
              help='Display resources which matches the SQL where clause (ex: "state = \'Suspected\'")')
@click.option('-s', '--state', help='show the states of the nodes')
@click.option('-l', '--list', help='show the nodes list')
@click.option('-e', '--events',
              help='show the events recorded for a node either since the date given as parameter or the last 20 minutes')
@click.option('-J', '--json', help='print result in JSON format')
@click.option('-V', '--version',  help='Print OAR version.')
def cli(resource, state, liste, events, sql, json, version, cli=True):
    pass
