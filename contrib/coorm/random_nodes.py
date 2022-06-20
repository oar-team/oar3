#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, with_statement

from random import randint

import click

from oar.kao.coorm import CoormApplication
from oar.lib import config

click.disable_unicode_literals_warning = True


class RandomNodes(CoormApplication):
    def __init__(self, **kwargs):
        super(RandomNodes, self).__init__(**kwargs)
        self.ref_walltime = self.walltime

    @property
    def rqt_nodes(self):
        if isinstance(self.nodes_range, list):
            return randint(self.nodes_range[0], self.nodes_range[1])
        else:
            return self.nodes_range

    def get_moldable_resources_requests(self, job):
        # Change resources request randomly
        for mld_res_rqt in job.mld_res_rqts:
            mld_res_rqt[2][0][0][0][1] = self.rqt_nodes
        return job.mld_res_rqts

    def submit(self):
        """Submit a new OAR job from the Rest API"""
        self.walltime = self.ref_walltime * randint(1, 10)
        self.command = "sleep %s" % (randint(1, self.walltime))
        super(RandomNodes, self).submit()

    def assign_resources(self, slots_set, job, hy, min_start_time):
        """Assign resources to a job and update by splitting the concerned
        slots"""
        job.mld_res_rqts = self.get_moldable_resources_requests(job)
        from oar.kao.scheduling import assign_resources_mld_job_split_slots

        ret = None
        while ret is None:
            ret = assign_resources_mld_job_split_slots(
                slots_set, job, hy, min_start_time
            )
        return ret


def get_default_bind():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return "tcp://%s:13296" % s.getsockname()[0]


@click.command()
@click.option("--zeromq-bind-uri", help="ZeroMQ bind uri", default=get_default_bind)
@click.option(
    "--api-host", default="http://localhost/oarapi-priv", help="OAR Rest API url"
)
@click.option("-u", "--username", default="docker")
@click.option("-p", "--password", default="docker")
@click.option("-w", "--walltime", default=10)
@click.option(
    "-n",
    "--nodes",
    default="1",
    help="Number of required nodes (may be a range like x:y)",
)
@click.option(
    "-s", "--submit-count", type=int, default=1, help="number of job submissions"
)
@click.option(
    "-i",
    "--submit-interval",
    type=int,
    default=10,
    help="iterval between two submissions",
)
def cli(
    zeromq_bind_uri,
    api_host,
    username,
    password,
    walltime,
    nodes,
    submit_count,
    submit_interval,
):
    """This application submits a job requiring a  random number of nodes for
    each OAR scheduler pass.
    It is a tool used to test the system stability in the worst case scenario.
    """
    if username == "" and password == "":
        api_credentials = None
    else:
        api_credentials = (username, password)
    config["LOG_FILE"] = None
    parts = nodes.split(":")
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        nodes_range = [int(parts[0]), int(parts[1])]
        max_nodes = max(nodes_range)
    elif nodes.isdigit():
        nodes_range = int(nodes)
        max_nodes = int(nodes)
    else:
        raise click.ClickException("Invalid range of nodes")

    command = "sleep %s" % walltime
    app = RandomNodes(
        command=command,
        api_host=api_host,
        zeromq_bind_uri=zeromq_bind_uri,
        api_credentials=api_credentials,
        nodes=max_nodes,
        walltime=walltime,
        nodes_range=nodes_range,
    )
    app.run(submit_count, submit_interval)


if __name__ == "__main__":
    cli()
