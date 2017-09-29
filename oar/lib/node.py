# coding: utf-8
from sqlalchemy import (func, text, distinct, or_, and_)
from oar.lib import (db, Resource, GanttJobsResource, GanttJobsPrediction, Job,
                     EventLog, EventLogHostname, MoldableJobDescription,
                     AssignedResource, get_logger)

import oar.lib.tools as tools

logger = get_logger('oar.lib.node')

def get_all_resources_on_node(hostname):
    """Return the current resources on node whose hostname is passed in parameter"""
    return db.query(Resource.id).filter(Resource.id == hostname).all()

def get_nodes_with_state(nodes):
    result = db.query(Resource.network_address, (Resource.state))\
           .filter(Resource.network_address.in_(tuple(nodes)))\
           .all()
    return result


def search_idle_nodes(date):
    result = db.query(distinct(Resource.network_address))\
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

#TODO MOVE TO GANTT
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
    result = db.query(distinct(Resource.network_address))\
               .filter(Resource.id == AssignedResource.resource_id)\
               .filter(AssignedResource.moldable_id == MoldableJobDescription.id)\
               .filter(MoldableJobDescription.job_id == Job.id)\
               .filter(Job.state.in_(('Waiting', 'Hold', 'toLaunch', 'toError', 'toAckReservation',
                                      'Launching', 'Running ', 'Suspended ', 'Resuming ')))\
               .filter(or_(Resource.state == 'Alive', Resource.next_state == 'Alive'))\
               .all()
    return result


def get_nodes_that_can_be_waked_up(date):
    """Returns the list nodes that can be waked up from to the given date"""
    result = db.query(distinct(Resource.network_address))\
               .filter(Resource.state == 'Absent')\
               .filter(Resource.available_upto > date)\
               .all()
    return result


def get_nodes_with_given_sql(properties):
    """Gets the nodes list with the given sql properties"""
    result = db.query(Resource.network_address)\
               .distinct()\
               .filter(text(properties))\
               .all()
    return result


def set_node_state(host, state, finaud_tag):
    """Sets the state field of some node identified by its hostname in the base.
    - parameters : base, hostname, state, finaudDecision
    - side effects : changes the state value in some field of the nodes table"""
    if state == 'Suspect':
        raise NotImplementedError(set_node_state)


def set_node_nextState(hostname, next_state):
    """Sets the nextState field of a node identified by its network_address"""
    db.query(Resource).filter(Resource.network_address == hostname).update(
        {Resource.next_state: next_state, Resource.next_finaud_decision: 'NO'})
    db.commit()

def change_node_state(node, state, config):
    """Changes node state and notify central automaton"""
    set_node_nextState(node, state)
    tools.notify_almighty('ChState')

def get_finaud_nodes():
    """Return the list of network address nodes for Finaud"""
    return  db.query(distinct(Resource.network_address))\
              .filter(or_(Resource.state == 'Alive', and_(Resource.state == 'Suspected',
                                                          Resource.finaud_decision == 'YES')))\
              .filter(Resource.type == 'default')\
              .filter(Resource.desktop_computing == 'NO')\
              .filter(Resource.next_state == 'UnChanged').all()

def get_current_assigned_nodes():
    """Returns the current nodes"""
    return db.query(distinct(Resource.network_address))\
             .filter(AssignedResource.index == 'CURRENT')\
             .filter(Resource.id == AssignedResource.resource_id)\
             .filter(Resource.type == 'default').all()

def update_node_nextFinaudDecision(network_address, finaud_decision):
    # Update nextFinaudDecision field
    db.query(Resource).filter(Resource.network_address == network_address)\
                      .update({Resource.next_finaud_decision: finaud_decision})
    db.commit()

    
