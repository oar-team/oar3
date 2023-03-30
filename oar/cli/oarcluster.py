# -*- coding: utf-8 -*-
import itertools

import click
import rich
from ClusterShell.NodeSet import NodeSet
from rich.columns import Columns
from rich.console import Console
from rich.padding import Padding
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Column, Table

from oar import VERSION
from oar.lib import AssignedResource, Job, Resource, db

from .utils import CommandReturns

console = Console()


def get_resources_for_job():
    res = (
        db.query(Resource, Job)
        .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)
        .filter(AssignedResource.resource_id == Resource.id)
        .filter(Job.state == "Running")
        .order_by(Resource.id)
        .all()
    )

    grouped = {k: list(g) for k, g in itertools.groupby(res, lambda t: t[0].id)}
    return grouped


def get_resources_grouped_by_network_address():
    """Return the current resources on node whose hostname is passed in parameter"""
    result = db.query(Resource).order_by(Resource.network_address, Resource.id).all()
    grouped = {
        k: list(g) for k, g in itertools.groupby(result, lambda t: t.network_address)
    }
    return grouped


def cluster_details(node_list, res_to_jobs):
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
            if r.id in res_to_jobs:
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


def cluster_summary(nodes_list, res_to_jobs):
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


@click.command(cls=click.Command)
@click.option("-V", "--version", is_flag=True, help="print OAR version number")
@click.option("-f", "--details", is_flag=True, help="Print details for every nodes")
def cli(version, details):
    cmd_ret = CommandReturns(cli)
    # Print OAR version and exit
    if version:
        cmd_ret.print_("OAR version : " + VERSION)
        cmd_ret.exit()

    node_list = get_resources_grouped_by_network_address()
    res_to_jobs = get_resources_for_job()

    if details:
        cluster_details(node_list, res_to_jobs)
    else:
        cluster_summary(node_list, res_to_jobs)

    cmd_ret.exit()
