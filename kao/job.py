from oar import db, Job, MoldableJobDescription, JobResourceDescription, JobResourceGroup, Resource
from interval import unordered_ids2itvs
class Job(Job):
    ''' Use 

        j1 = Job(1,"Waiting", 0, 0, "yop", "", "",{}, [], 0, 
                 [ 
                     (1, 60, 
                      [  ( [("node", 2)], [(1,32)]  ]
                  )
                 ]         
        )

    Attributes:

    mld_res_rqts: Resources requets by moldable instance
                  [                                    # first moldable instance
                     (1, 60,                           # moldable id, walltime 
                      [  ( [("node", 2)], [(1,32)] ) ] # list of requests composed of
                  )                                    # list of hierarchy request and filtered 
                 ]                                     # resources (Properties)
    

    '''
    def pseudo(self, start_time, walltime, res_set):
        self.start_time = start_time
        self.walltime = walltime
        self.res_set = res_set
        
    def set(self, id, state, start_time, walltime, user, name, project, types, res_set, \
                 moldable_id, mld_res_rqts, key_cache=""):
        self.id = id
        self.state = state
        self.start_time = start_time
        self.walltime = walltime
        self.user = user
        self.name = name
        self.project = project
        self.types = types
        self.res_set = res_set
        self.moldable_id = moldable_id
        self.mld_res_rqts = mld_res_rqts #[ (moldable_id, walltime, 
        #                                   [   [ (hy_level, hy_nb, constraints) ]  ]
        # hy_level = [ [string] ]
        # hy_nb = [ [ int ] ]
        # constraints = [ [itvs]  ]
        self.key_cache = key_cache
        if not key_cache:
            if len(mld_res_rqts) == 1:
                (m_id, walltime, res_rqt) = mld_res_rqts[0]
                self.key_cache = (str(walltime)).join(str(res_rqt))
            else:
                #TODO cache for moldable_id
                pass

def get_waiting_jobs(queue):
    #TODO  fairsharing_nb_job_limit
    waiting_jobs = {}
    waiting_jids = []   
    nb_waiting_jobs = 0

    for j in Job.query.filter(Job.state == "Waiting")\
                      .filter(Job.queue_name == queue)\
                      .filter(Job.reservation == 'None'):
        jid = int(j.id)
        waiting_jobs[jid] = j
        waiting_jids.append(jid)
        nb_waiting_jobs += 1

    return (waiting_jobs, waiting_jids, nb_waiting_jobs, resource_set)
        

def get_data_jobs(jobs, jids, rid_i2o):
    req = db.query(Job.id,\
                   MoldableJobDescription.id,\
                   MoldableJobDescription.walltime,\
                   JobResourceDescription.res_job_resource_type,\
                   JobResourceDescription.res_job_value,\
                   JobResourceDescription.res_job_order,\ #to remove ???
                   JobResourceGroup.res_group_property)\
            .filter(MoldableJobDescription.index == 'CURRENT')\
            .filter(JobResourceGroup.res_group_index == 'CURRENT')\
            .filter(JobResourceDescription.res_job_index == 'CURRENT')\
            .filter(Job.id.in_( tuple(jids) ))\
            .filter(Job.id == MoldableJobDescription.job_id)\
            .filter(JobResourceGroup.res_group_moldable_id == MoldableJobDescription.id)\
            .filter(JobResourceDescription.res_job_group_id == JobResourceGroup.res_group_id)\
            .order_by(MoldableJobDescription.id,\
                      JobResourceGroup.res_group_id, JobResourceDescription.res_job_order)\
            .all()

    cache_constraints = {}

    for x in req:
        print x
        j_id, mld_id, mld_id_walltime, res_type, res_value, res_order, res_grp_property = x #remove res_order
        job = jobs[j_id]

        #
        # determine resource constraints
        #
        if (job.properties == "" and (res_grp_property == "" or res_grp_property == "type = 'default'" )):
            res_constraints = resources_set.roid_itvs
        else:
            if job.properties == "" or res_grp_property == "":
                and_sql = ""
            else:
                and_sql = " AND "
            
            sql_constraints = job.properties + and_sql + res_grp_property

            if sql_constraints in cache_constraints:
                res_constraints = cache_constraints[sql_constraints]
            else:
                request_constraints = db.query(Resource.id).filter(sql_constraints).all() 
                roids = [ resource_set.rid_i2o(int(x[0])) for x in request_constraints ]
                roids_itvs = unordered_ids2itvs(roids)
                cache_constraints[sql_constraints] = res_constraints

def get_scheduled_jobs():
    pass
