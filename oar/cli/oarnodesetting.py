# -*- coding: utf-8 -*-
#
# Create a new resource or change the state and properties of existing resources.
#
# - The states allowed are: Alive, Absent or Dead.
# - If not specified, the hostname will be retrieved with the 'hostname' command.
# - "-a, --add" and "-r, --resource" or "--sql"  cannot be used at a same time.

from oar import (VERSION)
from oar.lib import (db, config)

from oar.lib.nodes import set_node_nextState

import oar.lib.tools as tools

from .utils import (CommandReturns, usage)

from socket import gethostname  

import click

click.disable_unicode_literals_warning = True


def set_resources_properties(resources, hostnames, properties):
    #TODO
    pass

def wait_end_of_running_jobs(jobs):
    #TODO
    pass
    
def set_maintenance(resources, maintenance_state, no_wait):
    #TODO
    pass

def oarnodesetting(resources, hostnames, filename, sql, add, maintenance, drain,
                   properties, no_wait, last_property_value, version):

    notify_server_tag_list = []
    
    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    if not (properties or state or add or maintenance or drain\
            or last_property_value):
        usage()

    if state and state not in ['Alive', 'Absent', 'Dead']:
        cmd_ret.warn('Bad state value. Possible values are: Alive | Absent | Dead')
        usage()

    if maintenance and maintenance not in ['on', 'off']:
        cmd_ret.warn('Bad maintenance mode value. Possible values are: on | off')
        usage()

    if drain and drain  not in ['on', 'off']:
        cmd_ret.warn('Bad drain mode value. Possible values are: on | off')
        usage()

    
    if sql:
        #TODO
        # if (defined($Sql_property)){
        #                 my $db = OAR::IO::connect_ro();
        #                 foreach my $r (OAR::IO::get_resources_with_given_sql($db,$Sql_property)){
        #                     push(@resources, $r);
        #                 }
        #         OAR::IO::disconnect($db);
        #         if ($#resources < 0){
        #             warn("/!\\ Your SQL clause returns nothing and there is no resource specified.\n");
        #         $exit_code = 12;
        #         exit($exit_code);
        # }       
        pass

    hosts = []
    # Get hostnames from a file
    try:
        with open(filename, 'r') as hostfile:
            hosts = [host for host in hostfile]
    except OSError as e:
        cmd_ret.warn(str(e), 13)

    hostsnames += hosts

    if add and (resources or sql):
        cmd_ret.warn('You cannot use -r|--resource or --sql and -a|--add options at a same time')
        usage()
    
    if not hostnames:
        hostnames = [gethostname()]

    if last_property_value:
        #TODO
        value = get_resource_last_value_of_property(last_property_value)
        if value:
            cmd_ret._print(str(value))
        else:
            cmd_ret.warn('Cannot retrieve the last value for ' + last_property_value\
                         + '. Either no resource or no such property exists (yet).')
            return cmd_ret

    elif add:
        # Create a new resource
        if not state:
            state = 'Alive'

        for host in hostnames:
            add_resource(host, state)
            cmd_ret._print('New resource added: ' + host)

        notify_server_tag_list.append('ChState')
        notify_server_tag_list.append('Term')

    else:
        if resources:
            if state:
                tmp_nb_updates = set_resources_nextState(resources, state)
                if tmp_nb_updates < len(resources):
                    cmd_ret.warn(str(len(resources) - tmp_nb_updates) +\
                                 ' resource(s) cannot be updated.', 3)
                else:
                    cmd_ret('(' + ','.join(resources) + ') --> ' + state)

                tools.notify_almighty('ChState')

                if (state in ['Dead', 'Absent']) and not no_wait:
                    for resource in resources:
                        cmd_ret._print('Check jobs to delete on resource: ' + resource)     
                        jobs = get_resource_job_to_frag(resource)
                        wait_end_of_running_jobs(jobs)
                elif state == 'Alive':
                    cmd_ret._print('Done')

            if maintenance:
                   set_maintenance(resources, maintenance, no_wait);
            
        else:
            # update all resources with netwokAdress = $hostname
            if maintenance:
                resources_to_maintain = []
                for host in hostnames:
                    resources_to_maintain += get_all_resources_on_node(host)

                set_maintenance(resources_to_maintain, maintenance, no_wait)

            if state:
                hosts_to_check = []
                for host in hostnames:
                    if set_node_nextState(host, state):
                        cmd_ret._print(host + ' --> ' + state)
                        hosts_to_check.append(host)
                    else:
                        cmd_ret.warn('Node ' + host\
                                     + ' does not exist in OAR database.', 4)
                tools.notify_almighty('ChState')

                if (state in ['Dead', 'Absent']) and not no_wait:
                    for hosts in hosts_to_check:
                        cmd_ret._print('Check jobs to delete on host: ' + host)
                        jobs = get_node_job_to_frag(host)
                        wait_end_of_running_jobs(jobs)

    if drain:
        if drain == 'on':
            properties.append('drain=YES')
        elif drain == 'off':
            properties.append('drain=NO')
                              
    # Update properties
    if properties:
        if resources:
            set_resources_properties(resources, None, properties)
        elif hostnames:
            set_resources_properties(None, hostnames, properties)
        else:
            cmd_ret.warn('Cannot find resources to set in OAR database.', 2)

        tools.notify_almighty('Term')

    for tag in notify_server_tag_list:
        tools.notify_almighty(tag)
            
    return cmd_ret

@click.command()

@click.option('-r', '--resource', type=click.INT, multiple=True,
              help='Resource id of the resource to modify')
@click.option('-h', '--hostname', type=click.STRING, multiple=True,
              help='hostname for the resources to modify')
@click.option('-f', '--file', type=click.STRING,
              help='Get a hostname list from a file (1 hostname by line) for resources to modify')
@click.option('--sql', type=click.STRING,
              help='Select resources to modify from database using a SQL where clause on the resource table (e.g.: "type = \'default\'")')
@click.option('-a','--add', is_flag=True, help='Add a new resource')
@click.option('-s','--state', type=click.STRING, help='Set the new state of the node')
@click.option('-m','--maintenance', type=click.STRING,
              help='Set/unset maintenance mode for resources, this is equivalent to setting its state to Absent and its available_upto to 0')
@click.option('-d', '--drain', type=click.STRING,
              help='Prevent new job to be scheduled on resources, this is equivalent to setting the drain property to YES')
@click.option('-p', '--property', type=click.STRING, multiple=True,
              help='Set the property of the resource to the given value')
@click.option('-n', '--no-wait', is_flag=True,
              help='Do not wait for job end when the node switches to Absent or Dead')
@click.option('--last-property-value', type=click.STRING,
              help='Get the last value used for a property (as sorted by SQL\'s ORDER BY DESC)')
#@click.option('--verbose', is_flag=True, help='Verbose output')
@click.option('-V', '--version', help='Print OAR version number')

def cli(resource, hostname, file, sql, add, state, maintenance, drain, property,
        no_wait, last_property_value, version):
    resources=resources
    hostnames=hostname
    filename=file
    properties=property
    cmd_ret = oarnodesetting(resources, hostnames, filename, sql, add, state,
                             maintenance, drain, properties, no_wait,
                             last_property_value, version)
    cmd_ret.exit()