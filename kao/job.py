from oar import db, Job, MoldableJobDescription, JobResourceDescription, JobResourceGroup

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

    return (waiting_jobs, waiting_jids, nb_waiting_jobs)
        

def get_data_jobs(jobs, jids):
    req = db.query(Job.id,\
                   MoldableJobDescription.id,\
                   MoldableJobDescription.walltime,\
                   JobResourceDescription.res_job_resource_type,\
                   JobResourceDescription.res_job_value,\
                   JobResourceDescription.res_job_order,\
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

    for x in req:
        print x

#  let query_base = Printf.sprintf "
#    SELECT jobs.job_id, moldable_job_descriptions.moldable_walltime, jobs.properties,
#        moldable_job_descriptions.moldable_id,
#        job_resource_descriptions.res_job_resource_type,
#        job_resource_descriptions.res_job_value,
#        job_resource_descriptions.res_job_order,
#        job_resource_groups.res_group_property,
#        jobs.job_user,
#        jobs.project
#    FROM moldable_job_descriptions, job_resource_groups, job_resource_descriptions, jobs
#    WHERE
#      moldable_job_descriptions.moldable_index = 'CURRENT'
#      AND job_resource_groups.res_group_index = 'CURRENT'
#      AND job_resource_descriptions.res_job_index = 'CURRENT' "
#  and query_end = "

#      AND jobs.job_id = moldable_job_descriptions.moldable_job_id
#      AND job_resource_groups.res_group_moldable_id = moldable_job_descriptions.moldable_id
#      AND job_resource_descriptions.res_job_group_id = job_resource_groups.res_group_id

#      ORDER BY moldable_job_descriptions.moldable_id, job_resource_groups.res_group_id, job_resource_descriptions.res_job_order ASC;"

#      (* ORDER BY job_resource_descriptions.res_job_order DESC; *)
#  in
#    let query =
#      if fairsharing_flag then
#        query_base ^ " AND jobs.job_id IN (" ^ (Helpers.concatene_sep "," id fs_jobids) ^ ") " ^ query_end
#      else
#        query_base ^ " AND jobs.state = 'Waiting' AND jobs.queue_name = '" ^ queue ^"' "^ query_end

def get_scheduled_jobs():
    pass
