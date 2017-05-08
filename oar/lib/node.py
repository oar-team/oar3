# coding: utf-8

#TODO move to oar.lib

from __future__ import unicode_literals, print_function

from sqlalchemy import func
from oar.lib import (db, Resource, GanttJobsResource, GanttJobsPrediction, Job,
                     EventLog, EventLogHostname, MoldableJobDescription,
                     AssignedResource, get_logger)

logger = get_logger("oar.kao.node")


def get_nodes_with_state(nodes):
    result = db.query(Resource.network_address, (Resource.state))\
           .filter(Resource.network_address.in_(tuple(nodes)))\
           .all()
    return result


def search_idle_nodes(date):
    result = db.query(Resource.network_address).distinct()\
               .filter(Resource.id == GanttJobsResource.resource_id)\
               .filter(GanttJobsPrediction.start_time <= date)\
               .filter(Resource.network_address != '')\
               .filter(Resource.type == 'default')\
               .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)\

    busy_nodes = {} #TODO can be remove ? to replace by busy_nodes = result
    for network_address in result:
        busy_nodes[network_address] = True

    result = db.query(Resource.network_address,
                      func.max(Resource.last_job_date))\
               .filter(Resource.state == 'Alive')\
               .filter(Resource.network_address != '')\
               .filter(Resource.type == 'default')\
               .filter(Resource.available_upto < 2147483647)\
               .filter(Resource.available_upto > 0)\
               .group_by(Resource.network_address)\
               .all()

    idle_nodes = {}
    for x in result:
        network_address, last_job_date = x
        if network_address not in busy_nodes:
            idle_nodes[network_address] = last_job_date

    return idle_nodes


def get_gantt_hostname_to_wake_up(date, wakeup_time):
    '''Get hostname that we must wake up to launch jobs'''
    hostnames = db.query(Resource.network_address)\
                  .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
                  .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
                  .filter(Job.id == MoldableJobDescription.job_id)\
                  .filter(GanttJobsPrediction.start_time <= date + wakeup_time)\
                  .filter(Job.state == 'Waiting')\
                  .filter(Resource.id == GanttJobsResource.resource_id)\
                  .filter(Resource.state == 'Absent')\
                  .filter(Resource.network_address != '')\
                  .filter(Resource.type == 'default')\
                  .filter((GanttJobsPrediction.start_time + MoldableJobDescription.walltime) <=
                          Resource.available_upto)\
                  .group_by(Resource.network_address)\
                  .all()
    hosts = [h_tpl[0] for h_tpl in hostnames]
    return hosts


def get_next_job_date_on_node(hostname):
    result = db.query(func.min(GanttJobsPrediction.start_time))\
               .filter(Resource.network_address == hostname)\
               .filter(GanttJobsResource.resource_id == Resource.id)\
               .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)\
               .scalar()

    return result


def get_last_wake_up_date_of_node(hostname):
    result = db.query(EventLog.date)\
               .filter(EventLogHostname.event_id == EventLog.id)\
               .filter(EventLogHostname.hostname == hostname)\
               .filter(EventLog.type == 'WAKEUP_NODE')\
               .order_by(EventLog.date.desc()).limit(1).scalar()

    return result

def get_alive_nodes_with_jobs():
    """Returns the list of occupied nodes"""
    result = db.query(Resource.network_address).distinct()\
               .filter(Resource.id == AssignedResource.resource_id)\
               .filter(AssignedResource.moldable_id == MoldableJobDescription.moldable_id)\
               .filter(MoldableJobDescription.moldable_id == Job.id)\
               .filter(Job.state.in_(('Waiting', 'Hold', 'toLaunch', 'toError', 'toAckReservation',
                                      'Launching', 'Running ', 'Suspended ', 'Resuming ')))\
               .filter(or_(Resource.state == 'Alive', Resource.next_state == 'Alive'))\
               .all()
    return result

