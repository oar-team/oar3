from sqlalchemy import func 
from oar.lib import (db, Resource, GanttJobsResource, GanttJobsPrediction, get_logger)

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

    nodes_occupied = {} 
    for network_address in result:
        if network_address not in nodes_occupied:
            nodes_occupied[network_address] = True

    result = db.query(Resource.network_address, func.max(Resource.last_job_date))\
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
        if network_address not in nodes_occupied:
            idle_nodes[network_address] = last_job_date
 
    return idle_nodes
