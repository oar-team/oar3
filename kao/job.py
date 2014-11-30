from oar import Job
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

    for j in Job.query.filter(Job.state == "Waiting").filter(Job.queue == queue):
        jid = int(j.id)
        waiting_jobs[jid] = j
        waiting_jids.append(jid)
        nb_waiting_jobs += 1

    return (waiting_jobs, waiting_jids, nb_waiting_jobs)
        
