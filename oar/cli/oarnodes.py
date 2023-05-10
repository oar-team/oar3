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
# -*- coding: utf-8 -*-
import itertools
import sys
from json import dumps

import click
import rich
from ClusterShell.NodeSet import NodeSet
from rich.columns import Columns
from rich.console import Console
from rich.padding import Padding
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Column, Table
from sqlalchemy.orm import scoped_session, sessionmaker

import oar.lib.tools as tools
from oar import VERSION
from oar.lib.event import get_events_for_hostname_from
from oar.lib.globals import init_oar
from oar.lib.models import AssignedResource, Job, Resource
from oar.lib.node import (
    get_all_network_address,
    get_resources_of_nodes,
    get_resources_state_for_host,
)
from oar.lib.resource_handling import (
    get_resources_from_ids,
    get_resources_state,
    get_resources_with_given_sql,
)
from oar.lib.tools import check_resource_system_property, local_to_sql

from .utils import CommandReturns

click.disable_unicode_literals_warning = True


def get_resources_for_job(session):
    res = (
        session.query(Resource, Job)
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .filter(AssignedResource.resource_id == Resource.id)
        .filter(Job.state == "Running")
        .order_by(Resource.id)
        .all()
    )

    grouped = {k: list(g) for k, g in itertools.groupby(res, lambda t: t[0].id)}
    return grouped


def get_resources_grouped_by_network_address(session, hostnames=[]):
    """Return the current resources on node whose hostname is passed in parameter"""
    result = (
        session.query(Resource)
        .filter(Resource.network_address.in_(hostnames))
        .order_by(Resource.network_address, Resource.id)
        .all()
    )
    grouped = {
        k: list(g) for k, g in itertools.groupby(result, lambda t: t.network_address)
    }
    return grouped


def print_events(session, date, hostnames, json):
    console = Console()
    if not json:
        table = Table()
        table.box = rich.box.SIMPLE_HEAD
        table.row_styles = ["none", "dim"]

        table.add_column("Date")
        table.add_column("hostname")
        table.add_column("Job id")
        table.add_column("Type")
        table.add_column("Description")

        for hostname in hostnames:
            events = get_events_for_hostname_from(session, hostname, date)
            for ev in events:
                table.add_row(
                    str(local_to_sql(ev.date)),
                    str(hostname),
                    str(ev.job_id),
                    str(ev.type),
                    str(ev.description),
                )
            table.add_section()
        console.print(table)
    else:
        hosts_events = {
            hostname: [
                ev.to_dict()
                for ev in get_events_for_hostname_from(session, hostname, date)
            ]
            for hostname in hostnames
        }
        console.print_json(dumps(hosts_events))


def print_resources_states(session, resource_ids, json):
    resource_states = get_resources_state(session, resource_ids)
    if not json:
        for resource_state in resource_states:
            resource_id, state = resource_state.popitem()
            print("{}: {}".format(resource_id, state))
    else:
        print(dumps(resource_states))


def print_resources_states_for_hosts(session, hostnames, json, show_jobs=False):
    if not json:
        node_list = get_resources_grouped_by_network_address(session, hostnames)
        res_to_jobs = get_resources_for_job(session)
        cluster_details(node_list, res_to_jobs, False)
    else:
        hosts_states = [
            {hostname: get_resources_state_for_host(session, hostname)}
            for hostname in hostnames
        ]
        print(dumps(hosts_states))


def print_all_hostnames(nodes, json):
    if not json:
        for hostname in nodes:
            print(hostname)
    else:
        print(dumps(nodes))


# INFO: function to change if you want to change the user std output
def print_resources_flat_way(session, cmd_ret, resources):
    now = tools.get_date(session)

    properties = [column.name for column in Resource.columns]

    for resource in resources:
        cmd_ret.print_("network_address: " + resource.network_address)
        cmd_ret.print_("resource_id: " + str(resource.id))
        state = resource.state
        if state == "Absent" and resource.available_upto >= now:
            state += " (standby)"
        cmd_ret.print_("state: " + state)
        properties_str = "properties: "
        flag_comma = False
        for prop_name in properties:
            if not check_resource_system_property(prop_name):
                if flag_comma:
                    properties_str += ", "
                v = getattr(resource, prop_name)
                properties_str += prop_name + "="
                if v:
                    properties_str += str(v)
                flag_comma = True
        cmd_ret.print_(properties_str)


def print_resources_table(
    cmd_ret, resources, properties_to_display=["cpu"], show_all=False
):
    table = Table()
    table.box = rich.box.SIMPLE_HEAD
    table.row_styles = ["none", "dim"]

    table.add_column("Id")
    table.add_column("Network address")
    table.add_column("State")
    table.add_column("Available upto")
    show_properties = properties_to_display or show_all

    if show_properties:
        properties = [
            column.name
            for column in Resource.columns
            if not check_resource_system_property(column.name)
            and (column.name in properties_to_display or show_all)
        ]

        for prop in properties:
            table.add_column(prop)

    for resource in resources:
        row = [
            str(resource.id),
            resource.network_address,
            resource.state,
            str(resource.available_upto),
        ]
        if show_properties:
            for prop in properties:
                value = getattr(resource, prop)
                if value:
                    row.append(str(value))
                else:
                    row.append("")

        color = ""
        if resource.state != "Alive":
            color = "red"

        table.add_row(*row, style=color)

    console = Console()
    console.print()
    console.print(table)


def print_resources_nodes_infos(
    session, cmd_ret, properties, show_all_properties, resources, nodes, json
):
    # import pdb; pdb.set_trace()
    if nodes:
        resources = get_resources_of_nodes(session, nodes)

    if not json:
        print_resources_table(
            cmd_ret,
            resources,
            properties,
            show_all_properties,
        )
        # print_resources_flat_way(cmd_ret, resources)
    else:
        print(dumps([r.to_dict() for r in resources]))


def cluster_summary(nodes_list, res_to_jobs):
    console = Console()
    sum_table = Table(expand=False)

    nodes = nodes_list.keys()
    # Load into clustershell so we have a natural sort
    n = NodeSet.fromlist(nodes)

    sum_table.add_column("node")
    sum_table.add_column("status")
    sum_table.add_column("running jobs")
    sum_table.add_column("free resources")

    for net in list(n):
        node_resources = nodes_list[net]
        # State should be consistent across resources of the same network_address
        first_resources = node_resources[0]
        node_state = first_resources.state

        bar_style = "bar.back"
        node_color = "blue"
        if node_state != "Alive":
            bar_style = "dark_red"
            node_color = "dark_red"

        number_of_resources = len(node_resources)
        number_of_free_resources = 0
        number_of_busy_resources = 0
        number_of_absent_resources = 0

        running_jobs = []
        for r in node_resources:
            resource_representation = ""
            if r.id in res_to_jobs:
                running_jobs.extend(res_to_jobs[r.id])
                number_of_busy_resources += 1
                node_color = "cyan"
            elif r.state != "Alive":
                number_of_absent_resources += 1

        number_of_free_resources = (
            number_of_resources - number_of_busy_resources - number_of_absent_resources
        )

        if number_of_free_resources == 0 and node_state == "Alive":
            node_color = "white"

        text_column = TextColumn(
            "[progress.description]{task.description}", table_column=Column(ratio=1)
        )
        bar_column = BarColumn(
            style=bar_style,
            complete_style="cyan",
            finished_style="blue",
            bar_width=None,
            table_column=Column(ratio=9),
        )
        # progress = Progress(text_column, bar_column, *Progress.get_default_columns(), expand=True)
        progress = Progress(text_column, bar_column, expand=True)

        progress.add_task(
            "{0:3d} /{1:3d}".format(number_of_free_resources, number_of_resources),
            total=number_of_resources,
        )
        progress.advance(0, number_of_free_resources)

        sum_table.add_row(
            f"[{node_color}]{net}",
            f"{node_state}",
            ", ".join(set([str(res_and_job[1].id) for res_and_job in running_jobs])),
            progress,
        )

        # Extended
        table = Table.grid(expand=True)
        table.add_column("node", justify="left", no_wrap=True)
        table.add_column("resources", justify="right")

        tablecontent = []
        for r in node_resources:
            resource_representation = ""
            if r.id in res_to_jobs:
                job = res_to_jobs[r.id][0][1]
                resource_representation += f"[orange]{job.id}[/orange]"
            elif r.state == "Alive":
                resource_representation += f"[green]{r.state}[/green]"
            else:
                resource_representation += "[green] [/green]"

            table.add_row(f"{r.id}", f"{resource_representation}")
            tablecontent.append(f"{resource_representation}")

    console.print(sum_table)


def cluster_details(node_list, res_to_jobs, show_jobs=False):
    console = Console()
    content = []
    nodes = node_list.keys()
    # Load into clustershell so we have a natural sort
    n = NodeSet.fromlist(nodes)

    for net in list(n):
        node_resources = node_list[net]
        # State should be consistent across resources of the same network_address
        first_resources = node_resources[0]
        node_state = first_resources.state

        node_color = "blue"
        if node_state != "Alive":
            node_color = "dark_red"

        # Extended
        table = Table.grid(expand=True)
        table.box = rich.box.SIMPLE_HEAD
        table.row_styles = ["none", "dim"]

        table.add_column("resource_id", justify="left")
        table.add_column("resources", justify="right")

        tablecontent = []
        for r in node_resources:
            resource_representation = ""
            if r.id in res_to_jobs and show_jobs:
                job_ids = ", ".join([str(j[1].id) for j in res_to_jobs[r.id]])
                resource_representation += f"[orange]{job_ids}[/orange]"
            elif r.state == "Alive":
                resource_representation += f"[blue]{r.state}[/blue]"
            else:
                resource_representation += f"[red]{r.state}[/red]"

            table.add_row(
                f"{r.id}", Padding(f"{resource_representation}", (0, 0, 0, 4))
            )
            tablecontent.append(f"{resource_representation}")

        p = Panel(table, title=f"[{node_color}]{net}", title_align="left")
        content.append(p)

    columns = Columns(content, equal=False, expand=False)

    console.print(columns)


def oarnodes(
    session,
    config,
    nodes,
    properties,
    show_all_properties,
    resource_ids,
    state,
    summary,
    list_nodes,
    events,
    sql,
    json,
    version,
    detailed=False,
):
    cmd_ret = CommandReturns(cli)

    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        return cmd_ret

    if (not nodes and not (resource_ids or sql)) or list_nodes:
        nodes = get_all_network_address(session)

    if sql:
        sql_resource_ids = get_resources_with_given_sql(session, sql)
        if not sql_resource_ids:
            cmd_ret.warning(
                "There are no resource(s) for this SQL WHERE clause ({})".format(sql),
                12,
            )
        resource_ids = resource_ids + tuple(sql_resource_ids)

    if events:
        if events == "_events_without_date_":
            events = None  # To display the 30's latest events
        print_events(session, events, nodes, json)
    elif summary:
        node_list = get_resources_grouped_by_network_address(session, nodes)
        res_to_jobs = get_resources_for_job(session)
        cluster_summary(node_list, res_to_jobs)
    elif state:
        if resource_ids:
            print_resources_states(session, resource_ids, json)
        else:
            # cluster_details(node_list, res_to_jobs)
            print_resources_states_for_hosts(session, nodes, json)
    elif list_nodes:
        print_all_hostnames(nodes, json)
    elif resource_ids or sql:
        resources = get_resources_from_ids(session, resource_ids)
        print_resources_nodes_infos(
            session, cmd_ret, properties, show_all_properties, resources, None, json
        )
    elif nodes:
        print_resources_nodes_infos(
            session, cmd_ret, properties, show_all_properties, None, nodes, json
        )
    else:
        cmd_ret.print_("No nodes to display...")
        # resources = db.query(Resource).order_by(Resource.id).all()
    return cmd_ret


def events_option_flag_or_string():
    """Click seems unable to manage option which is of type flag or string, _this_user_ is added to
    sys.argv when --user is used as flag , by example:
      -u --accounting "1970-01-01, 1970-01-20" -> -u _this_user_ --accounting "1970-01-01, 1970-01-20"
    """
    argv = []
    for i in range(len(sys.argv) - 1):
        a = sys.argv[i]
        argv.append(a)
        if (a == "-e" or a == "--events") and ((sys.argv[i + 1])[0] == "-"):
            argv.append("_events_without_date_")

    argv.append(sys.argv[-1])
    if (sys.argv[-1] == "-e") or (sys.argv[-1] == "--events"):
        argv.append("_events_without_date_")
    sys.argv = argv


class EventsOption(click.Command):
    def __init__(self, name, callback, params, help):
        events_option_flag_or_string()
        click.Command.__init__(self, name=name, callback=callback, params=params)


# @click.option('-f', '--full', is_flag=True, default=True, help='show full informations')
@click.command(cls=EventsOption)
@click.option(
    "-p",
    "--property",
    type=click.STRING,
    multiple=True,
    help="Show the specified properties",
)
@click.option("-P", "--show-all-properties", is_flag=True)
@click.argument("nodes", nargs=-1)
@click.option(
    "-r",
    "--resource",
    type=click.INT,
    multiple=True,
    help="show the properties of the resource whose id is given as parameter",
)
@click.option(
    "--sql",
    type=click.STRING,
    help="Display resources which matches the SQL where clause (ex: \"state = 'Suspected'\")",
)
@click.option("-s", "--state", is_flag=True, help="show the states of the nodes")
@click.option(
    "-S", "--Summary", is_flag=True, help="Show a summarized view of the cluster"
)
@click.option("-l", "--list", is_flag=True, help="show the nodes list")
@click.option(
    "-e",
    "--events",
    type=click.STRING,
    help="show the events recorded for a node either since the date given as parameter or the last 30 ones if date is not provided.",
)
@click.option(
    "-J", "--json", is_flag=True, default=False, help="print result in JSON format"
)
@click.option("-V", "--version", is_flag=True, help="Print OAR version.")
def cli(
    nodes,
    property,
    show_all_properties,
    resource,
    state,
    summary,
    list,
    events,
    sql,
    json,
    version,
    cli=True,
):
    """Display informations about nodes."""
    ctx = click.get_current_context()
    if ctx.obj:
        (session, config) = ctx.obj
    else:
        config, engine, log = init_oar()

        session_factory = sessionmaker(bind=engine)
        scoped = scoped_session(session_factory)
        session = scoped()

    properties = property
    cmd_ret = oarnodes(
        session,
        config,
        nodes,
        properties,
        show_all_properties,
        resource,
        state,
        summary,
        list,
        events,
        sql,
        json,
        version,
    )
    cmd_ret.exit()
