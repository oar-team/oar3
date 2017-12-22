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
from oar import (VERSION)
from oar.lib import (db, config)

from oar.lib.tools import check_resource_system_property
import oar.lib.tools as tools

from .utils import CommandReturns

import click

click.disable_unicode_literals_warning = True


# INFO: function to change if you want to change the user std output
def print_resources_flat_way(resources, resources_jobs, cmd_ret):
    now = tools.get_date()
    for resource in resources:
        cmd_ret.print_('network_address: ' + resource.network_address)
        cmd_ret.print_('resource_id: ' + str(resource.id))

        state = resource.state
        if state == 'Absent' and resource.available_upto >= now:
            state += ' (standby)'
        cmd_ret.print_('state: ' + state)

        #TODO: if (exists($info->{jobs})){print "jobs: $info->{jobs}\n"
	#    my $properties_to_display='';
        # 	    while ( my ($k,$v) = each %$info ){
        # 		    if (OAR::Tools::check_resource_system_property($k) == 0){
        # 			    if(defined($v)){
        # 				    $properties_to_display .= "$k=$v, ";
        # 			    }else{
        # 				    $properties_to_display .= "$k=, ";
        # 			    }
        # 		    }
        # 	    }
        # 	    chop($properties_to_display); # remove last space
        # 	    chop($properties_to_display); # remove last ,
        # 	    print "properties : $properties_to_display\n\n";
        # 	}
        # }
        

def oarnodes(resource_ids, states, list_nodes, events, sql, json, version, detailed):

    #config.setdefault_config(DEFAULT_CONFIG)

    cmd_ret = CommandReturns(cli)      

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    if sql:
        #TODO
        pass

    if resource_ids == ():
        resource_ids = None

    resources = db.queries.get_resources(resource_ids, detailed)

    print_resources_flat_way(resources, None, cmd_ret)

    return cmd_ret

@click.command()

@click.option('-r', '--resource', type=click.INT, multiple=True,
              help='show the properties of the resource whose id is given as parameter')
@click.option('--sql', type=click.STRING,
              help='Display resources which matches the SQL where clause (ex: "state = \'Suspected\'")')
@click.option('-s', '--state',  type=click.STRING, multiple=True, help='show the states of the nodes')
@click.option('-l', '--list', is_flag=True, help='show the nodes list')
@click.option('-e', '--events', type=click.STRING,
              help='show the events recorded for a node either since the date given as parameter or the last 20 minutes')
@click.option('-f', '--full', is_flag=True, default=True, help='show full informations')
@click.option('-J', '--json', help='print result in JSON format')
@click.option('-V', '--version',  help='Print OAR version.')
def cli(resource, state, list, events, sql, json, version, full, cli=True):
    cmd_ret = oarnodes(resource, state, list, events, sql, json, version, full)
    cmd_ret.exit()
