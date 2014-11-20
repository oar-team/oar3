class Job:
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
    def __init__(self, id, state, start_time, walltime, user, name, project, types, res_set, \
                 moldable_id, mld_res_rqts):
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
    
