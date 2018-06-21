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
import sys
from json import dumps

from oar import (VERSION)
from oar.lib import (db, config, Resource)

from oar.lib.resource_handling import (get_resources_with_given_sql, get_resources_from_ids)
from oar.lib.node import get_all_network_address
from oar.lib.event import get_events_for_hostname_from

from oar.lib.tools import (check_resource_system_property, local_to_sql)

import oar.lib.tools as tools

from .utils import CommandReturns

import click

click.disable_unicode_literals_warning = True


def print_events(date, hostnames, json):
    if not json:
        for hostname in hostnames:
            events = get_events_for_hostname_from(hostname, date)
            for ev in events:
                print('{}| {}| {}: {}'.format(local_to_sql(ev.date), ev.job_id, ev.type, ev.description))
    else:
        hosts_events = {hostname: [ev.to_dict() for ev in get_events_for_hostname_from(hostname, date)]\
                        for hostname in hostnames}
        print(dumps(hosts_events))


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
        

def oarnodes(nodes, resource_ids, states, list_nodes, events, sql, json, version, detailed):

    config.setdefault_config(tools.DEFAULT_CONFIG)

    cmd_ret = CommandReturns(cli)      

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    if not nodes and not (resource_ids or sql):
        nodes = get_all_network_address()

    if sql:
        sql_resource_ids = get_resources_with_given_sql(sql)
        if not sql_resource_ids:
            cmd_ret.warning("There are no resource(s) for this SQL WHERE clause ({})".format(sql), 12)
        resource_ids = resource_ids + tuple(sql_resource_ids)

    if events:
        if events == '_events_without_date_': 
            events = None # To display the 30's latest events
        print_events(events, nodes, json)
    elif state:
        if resource_ids:
            print_resources_states(resources_ids)
        else:
            print_resources_states_for_hosts(nodes)
    elif list_nodes:
        print_all_hostnames()
    elif resource_ids or sql:
        resources = get_resources_from_ids(resource_ids)
        print_resources_flat_way(cmd_ret, resources, None)
    elif nodes:
        print_hosts_infos(nodes)
        
    else:
        cmd_ret.print_('No nodes to display...')
        #resources = db.query(Resource).order_by(Resource.id).all()
    return cmd_ret



def events_option_flag_or_string():
    """ Click seems unable to manage option which is of type flag or string, _this_user_ is added to 
    sys.argv when --user is used as flag , by example: 
      -u --accounting "1970-01-01, 1970-01-20" -> -u _this_user_ --accounting "1970-01-01, 1970-01-20"  
    """
    argv = []
    for i in range(len(sys.argv)-1):
        a = sys.argv[i]
        argv.append(a)
        if (a == '-e' or a == '--events') and ((sys.argv[i+1])[0] == '-'):
            argv.append('_events_without_date_')

    argv.append(sys.argv[-1])
    if (sys.argv[-1] == '-e') or (sys.argv[-1] == '--events'):
        argv.append('_events_without_date_')
    sys.argv = argv

class EventsOption(click.Command):
    def __init__(self,name,callback,params,help):
        events_option_flag_or_string()
        click.Command.__init__(self, name=name, callback=callback, params=params)

@click.command(cls=EventsOption)
@click.argument('nodes', nargs=-1)
@click.option('-r', '--resource', type=click.INT, multiple=True,
              help='show the properties of the resource whose id is given as parameter')
@click.option('--sql', type=click.STRING,
              help='Display resources which matches the SQL where clause (ex: "state = \'Suspected\'")')
@click.option('-s', '--state',  type=click.STRING, multiple=True, help='show the states of the nodes')
@click.option('-l', '--list', is_flag=True, help='show the nodes list')
@click.option('-e', '--events', type=click.STRING,
              help='show the events recorded for a node either since the date given as parameter or the last 30 ones if date is not provided.')
@click.option('-f', '--full', is_flag=True, default=True, help='show full informations')
@click.option('-J', '--json', is_flag=True, default=False, help='print result in JSON format')
@click.option('-V', '--version', is_flag=True, help='Print OAR version.')
def cli(nodes, resource, state, list, events, sql, json, version, full, cli=True):
    cmd_ret = oarnodes(nodes, resource, state, list, events, sql, json, version, full)
    cmd_ret.exit()
