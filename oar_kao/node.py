from sqlalchemy import func
from oar.lib import (db, Resource, GanttJobsResource, GanttJobsPrediction, Job,
                     get_logger)

log = get_logger("oar.kao")


def search_idle_nodes(date):
    result = db.query(Resource.network_address)\
               .filter(Resource.id == GanttJobsResource.resource_id)\
               .filter(GanttJobsPrediction.start_time <= date)\
               .filter(Resource.network_address != '')\
               .filter(Resource.type == 'default')\
               .filter(GanttJobsPrediction.moldable_id == GanttJobsResource.moldable_id)\
               .group_by(Resource.network_address)\
               .all()

    busy_nodes = {}
    for network_address in result:
        if network_address not in nodes_occupied:
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
                  .filter(Resource.state != 'Absent')\
                  .filter(Resource.network_address != '')\
                  .filter(Resource.type == 'default')\
                  .filter((GanttJobsPrediction.start_time + MoldableJobDescription.walltime) <=
                          Resource.available_upto)\
                  .group_by(Resource.network_address).all()

    return hostnames

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
