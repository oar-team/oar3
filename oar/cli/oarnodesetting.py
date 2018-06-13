# -*- coding: utf-8 -*-
#
# Create a new resource or change the state and properties of existing resources.
#
# - The states allowed are: Alive, Absent or Dead.
# - If not specified, the hostname will be retrieved with the 'hostname' command.
# - "-a, --add" and "-r, --resource" or "--sql"  cannot be used at a same time.

from oar import VERSION
from oar.lib import (db, config)

from oar.lib.node import set_node_nextState
from oar.lib.resource_handling import (set_resources_property, add_resource, 
                                       get_resources_with_given_sql, set_resources_nextState)
from oar.lib.tools import check_resource_system_property

import oar.lib.tools as tools

from .utils import CommandReturns

from socket import gethostname  

import click

click.disable_unicode_literals_warning = True


def set_resources_properties(cmd_ret, resources, hostnames, properties):
    for prop in properties:
        name_value = prop.lstrip().split('=')
        if len(name_value) == 2:
            name, value = name_value
            if check_resource_system_property(name):
                cmd_ret.warning('Cannot update property {} because it is a system field.'.format(name))
                cmd_ret.exit_values.append(8)
            else:
                cmd_ret.print_("Set property {} to '{}'...".format(name, value))
                ret = set_resources_property(resources, hostnames, name, value)
                cmd_ret.print_('{} resource(s) updated.'.format(ret))
                if ret <= 0:
                    cmd_ret.exit_values.append(9)
        else:
            cmd_ret.warning('Bad property syntax: {}\n'.format(name_value))
            cmd_ret.exit_values.append(10)

def wait_end_of_running_jobs(jobs):
    #TODO
    pass
    
def set_maintenance(resources, maintenance_state, no_wait):
    #TODO
    pass

def oarnodesetting(resources, hostnames, filename, sql, add, state, maintenance, drain,
                   properties, no_wait, last_property_value, version):
    notify_server_tag_list = []
    
    cmd_ret = CommandReturns()

    if version:
        cmd_ret.print_('OAR version : ' + VERSION)
        return cmd_ret

    if not (properties or state or add or maintenance or drain\
            or last_property_value):
        cmd_ret.warning('Option for setting (add, state, maintenance, drain, properties oar last-property-value) is expected')
        cmd_ret.usage(1)
        return cmd_ret
    
    if state and state not in ['Alive', 'Absent', 'Dead']:
        cmd_ret.warning('Bad state value. Possible values are: Alive | Absent | Dead')
        cmd_ret.usage(1)
        return cmd_ret
    
    if maintenance and maintenance not in ['on', 'off']:
        cmd_ret.warning('Bad maintenance mode value. Possible values are: on | off')
        cmd_ret.usage(1)
        return cmd_ret
    
    if drain and drain  not in ['on', 'off']:
        cmd_ret.warning('Bad drain mode value. Possible values are: on | off')
        cmd_ret.usage(1)
        return cmd_ret
    
    if sql:
        resources = get_resources_with_given_sql(sql)
        if not resources:
            cmd_ret.warning("There are no resource(s) for this SQL WHERE clause ({})".format(sql), 12)
            return cmd_ret

    hosts = []
    # Get hostnames from a file
    if filename:
        try:
            with open(filename, 'r') as hostfile:
                hosts = [host for host in hostfile]
        except OSError as e:
            cmd_ret.warning(str(e), 13)

        hostsnames += hosts

    if add and (resources or sql):
        cmd_ret.warning('You cannot use -r|--resource or --sql and -a|--add options at a same time')
        cmd_ret.usage(1)
        return cmd_ret
    
    if not hostnames:
        hostnames = [gethostname()]

    if last_property_value:
        #TODO
        value = get_resource_last_value_of_property(last_property_value)
        if value:
            cmd_ret.print_(str(value))
        else:
            cmd_ret.warning('Cannot retrieve the last value for ' + last_property_value\
                         + '. Either no resource or no such property exists (yet).')
            return cmd_ret

    elif add:
        # Create a new resource
        if not state:
            state = 'Alive'

        for host in hostnames:
            add_resource(host, state)
            cmd_ret.print_('New resource added: ' + host)

        notify_server_tag_list.append('ChState')
        notify_server_tag_list.append('Term')

    else:
        if resources:
            if state:
                nb_match_for_update = set_resources_nextState(resources, state)
                if nb_match_for_update < len(resources):
                    cmd_ret.warning(str(len(resources) - nb_match_for_update) +\
                                    ' resource(s) will be not updated (not exist ?).', 3)
                else:
                    cmd_ret.print_(str(resources) + ' --> ' + state)

                tools.notify_almighty('ChState')

                if (state in ['Dead', 'Absent']) and not no_wait:
                    for resource in resources:
                        cmd_ret.print_('Check jobs to delete on resource: ' + resource)     
                        jobs = get_resource_job_to_frag(resource)
                        wait_end_of_running_jobs(jobs)
                elif state == 'Alive':
                    cmd_ret.print_('Done')

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
                        cmd_ret.print_(host + ' --> ' + state)
                        hosts_to_check.append(host)
                    else:
                        cmd_ret.warning('Node ' + host\
                                     + ' does not exist in OAR database.', 4)
                tools.notify_almighty('ChState')

                if (state in ['Dead', 'Absent']) and not no_wait:
                    for hosts in hosts_to_check:
                        cmd_ret.print_('Check jobs to delete on host: ' + host)
                        jobs = get_node_job_to_frag(host)
                        wait_end_of_running_jobs(jobs)


    if drain:
        if drain == 'on':
            properties = properties + ('drain=YES',)
        elif drain == 'off':
            properties = properties + ('drain=NO',)

    # Update properties
    if properties:
        if resources:
            set_resources_properties(cmd_ret, resources, None, properties)
        elif hostnames:
            set_resources_properties(cmd_ret, None, hostnames, properties)
        else:
            cmd_ret.warning('Cannot find resources to set in OAR database.', 2)

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
@click.option('-V', '--version', is_flag=True, help='Print OAR version number')

def cli(resource, hostname, file, sql, add, state, maintenance, drain, property,
        no_wait, last_property_value, version):
    resources = resource
    hostnames = hostname
    filename = file
    properties = property
    cmd_ret = oarnodesetting(resources, hostnames, filename, sql, add, state,
                             maintenance, drain, properties, no_wait,
                             last_property_value, version)
    cmd_ret.exit()
