from oar import (db, Job, MoldableJobDescription, JobResourceDescription,
                 JobResourceGroup, Resource, GanttJobsPrediction, JobDependencie,
                 GanttJobsResource, JobType, JobStateLog, get_logger)

log = get_logger("oar.kamelot")

from interval import unordered_ids2itvs, itvs2ids, sub_intervals

''' Use

    j1 = Job(1,"Waiting", 0, 0, "yop", "", "",{}, [], 0,
                 [
                     (1, 60,
                      [  ( [("node", 2)], [(1,32)] )  ]
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

global NO_PLACEHOLDER
global SET_PLACEHOLDER
global USE_PLACEHOLDER

NO_PLACEHOLDER = 0
SET_PLACEHOLDER = 1
USE_PLACEHOLDER = 2

class JobPseudo():
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

#
# TODO to remove but address support for cache w/ moldable befor
#
def job_set(self, id, state, start_time, walltime, user, name, project, types, res_set, \
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

def get_jobs_types(jids, jobs):
    jobs_types = {}
    for j_type in JobType.query.filter(JobType.job_id.in_( tuple(jids) )):
        jid = j_type.job_id
        t_v = j_type.type.split("=")
        t = t_v[0]
        if t == "timesharing":
            job = jobs[jid] 
            job.ts = True
            job.ts_user, job.ts_jobname = t_v[1].split(',')
        elif t == "set_placeholder":
            job = jobs[jid] 
            job.ph = SET_PLACEHOLDER
            job.ph_name = t_v[1]
        elif t == "use_placeholder":
            job = jobs[jid]
            job.ph = USE_PLACEHOLDER
            job.ph_name = t_v[1]
        else:
            if len(tv) == 2:
                v = t_v[1]
            else:
                v = ""
            if not jid in jobs_types:
                jobs_types[jid] = dict()

            #print t, v
            (jobs_types[jid])[t] = v

    for job in jobs.itervalues():
        if job.id in jobs_types:
            job.types = jobs_types[job.id]
        else:
            job.types = {}

def get_data_jobs(jobs, jids, resource_set, job_security_time):
    '''
    oarsub -q test -l "nodes=1+{network_address='node3'}/nodes=1/resource_id=1" sleep
    job_id: 12 [(16L, 7200, [([(u'network_address', 1)], [(0, 7)]), ([(u'network_address', 1), (u'resource_id', 1)], [(4, 7)])])]

    '''

    req = db.query(Job.id,
                   MoldableJobDescription.id,
                   MoldableJobDescription.walltime,
                   JobResourceGroup.id,
                   JobResourceGroup.moldable_id,
                   JobResourceGroup.property,
                   JobResourceDescription.group_id,
                   JobResourceDescription.resource_type,
                   JobResourceDescription.value)\
            .filter(MoldableJobDescription.index == 'CURRENT')\
            .filter(JobResourceGroup.index == 'CURRENT')\
            .filter(JobResourceDescription.index == 'CURRENT')\
            .filter(Job.id.in_( tuple(jids) ))\
            .join(MoldableJobDescription)\
            .join(JobResourceGroup)\
            .join(JobResourceDescription)\
            .order_by(MoldableJobDescription.id,
                      JobResourceGroup.id,
                      JobResourceDescription.order)\
            .all()

    cache_constraints = {}

    first_job = True
    prev_j_id = 0
    prev_mld_id = 0
    prev_jrg_id = 0
    prev_res_jrg_id = 0
    mld_res_rqts = []
    jrg = []
    jr_descriptions = []
    res_constraints = []
    prev_mld_id_walltime = 0

    global job

    for x in req:
        j_id, mld_id, mld_id_walltime, jrg_id, jrg_mld_id, jrg_grp_property, res_jrg_id, res_type, res_value = x #remove res_order
        #print  x
        #
        # new job
        #
        if j_id != prev_j_id:
            if first_job:
                first_job = False
            else:
                jrg.append( (jr_descriptions, res_constraints) )
                mld_res_rqts.append( (prev_mld_id, prev_mld_id_walltime, jrg) )
                job.mld_res_rqts = mld_res_rqts
                job.key_cache = str(mld_res_rqts)
                mld_res_rqts = []
                jrg = []
                jr_descriptions = []
                job.ts = False
                job.ph = NO_PLACEHOLDER
                #print "======================"
                #print "job_id:",job.id,  job.mld_res_rqts
                #print "======================"

            prev_mld_id = mld_id
            prev_mld_id_walltime = mld_id_walltime + job_security_time
            prev_j_id = j_id
            job = jobs[j_id]

        else:
            #
            # new moldable_id
            #

            if mld_id != prev_mld_id:
                if jrg != []:
                    jrg.append( (jr_descriptions, res_constraints) )
                    mld_res_rqts.append( (prev_mld_id, prev_mld_id_walltime, jrg) )

                prev_mld_id = mld_id
                prev_mld_id_walltime = mld_id_walltime
                jrg = []
                jr_descriptions = []
        #
        # new job resources groupe_id
        #
        if jrg_id != prev_jrg_id:
            prev_jrg_id = jrg_id
            if jr_descriptions != []:
                jrg.append( (jr_descriptions, res_constraints) )
                jr_descriptions = []

        #
        # new set job descriptions
        #
        if res_jrg_id != prev_res_jrg_id:
            prev_res_jrg_id = res_jrg_id
            jr_descriptions = [ (res_type, res_value) ]

            #
            # determine resource constraints
            #
            if ( job.properties == "" and ( jrg_grp_property == "" or jrg_grp_property == "type = 'default'" ) ):
                res_constraints = resources_set.roid_itvs
            else:
                if job.properties == "" or  jrg_grp_property == "":
                    and_sql = ""
                else:
                    and_sql = " AND "

                sql_constraints = job.properties + and_sql + jrg_grp_property

                if sql_constraints in cache_constraints:
                    res_constraints = cache_constraints[sql_constraints]
                else:
                    request_constraints = db.query(Resource.id).filter(sql_constraints).all()
                    roids = [ resource_set.rid_i2o[ int(y[0]) ] for y in request_constraints ]
                    res_constraints = unordered_ids2itvs(roids)
                    cache_constraints[sql_constraints] = res_constraints
        else:
            # add next res_type , res_value
            jr_descriptions.append( (res_type, res_value) )
            #print "@@@@@@@@@@@@@@@@@@@"
            #print jr_descriptions

    # complete the last job
    jrg.append( (jr_descriptions, res_constraints) )
    mld_res_rqts.append( (prev_mld_id, prev_mld_id_walltime, jrg ) )

    job.mld_res_rqts = mld_res_rqts
    job.key_cache = str(mld_res_rqts)
    job.ts = False
    job.ph = NO_PLACEHOLDER

    #print "======================"
    #print "job_id:",job.id,  job.mld_res_rqts
    #print "======================"

    get_jobs_types(jids, jobs)


def get_job_suspended_sum_duration(jid, now):

    suspended_duration = 0
    for j_state_log in JobStateLog.query.filter(JobStateLog.job_id == jid)\
                                        .filter((JobStateLog.job_state == 'Suspended') | (JobStateLog.job_state == 'Resuming')):
        
        date_stop = j_state_log.date_stop
        date_start = j_state_log.date_start

        if date_stop == 0:
            res_time = now - date_start
        else:
            res_time = date_stop - date_start

        if res_time > 0:
            suspended_duration += res_time

    return suspended_duration

def get_scheduled_jobs(resource_set, job_security_time, now): #TODO available_suspended_res_itvs, now
    req = db.query(Job,
                   GanttJobsPrediction.start_time,
                   MoldableJobDescription.walltime,
                   GanttJobsResource.resource_id)\
            .filter(MoldableJobDescription.index == 'CURRENT')\
            .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
            .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
            .filter(Job.id == MoldableJobDescription.id)\
            .order_by(Job.start_time, Job.id)\
            .all()

    jids = []
    jobs_lst = []
    jobs = {}
    prev_jid = 0
    roids = []

    global job

    #(job, a, b, c) = req[0]
    if req != []:
        for x in req:
            (j, start_time, walltime, r_id) = x
            print x
            if j.id != prev_jid:
                if prev_jid != 0:
                    job.res_set = unordered_ids2itvs(roids)
                    jobs_lst.append(job)
                    jids.append(job.id)
                    jobs[job.id] = job
                    roids = []

                prev_jid = j.id
                job = j
                job.start_time = start_time
                job.walltime = walltime + job_security_time
                job.ts = False
                job.ph = NO_PLACEHOLDER
                if job.suspended == "YES":
                    job.walltime += get_job_suspended_sum_duration(job.id, now)

            roids.append(resource_set.rid_i2o[r_id])

        job.res_set = unordered_ids2itvs(roids)
        if job.state == "Suspended": 
            job.res_set = sub_intervals(job.res_set, resource_set.suspendable_roid_itvs)

        jobs_lst.append(job)
        jids.append(job.id)
        jobs[job.id] = job
        get_jobs_types(jids,jobs)

    return jobs_lst

def save_assigns(jobs, resource_set):
    #http://docs.sqlalchemy.org/en/rel_0_9/core/dml.html#sqlalchemy.sql.expression.Insert.values
    mld_id_start_time_s = []
    mld_id_rid_s = []
    for j in jobs.itervalues():
        mld_id_start_time_s.append( {'moldable_job_id': j.moldable_id, 'start_time': j.start_time} )
        riods = itvs2ids(j.res_set)
        mld_id_rid_s.extend( [{'moldable_job_id' : j.moldable_id, 'resource_id': resource_set.rid_o2i[rid]} for rid in riods] )

    log.info("save assignements")

    db.engine.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s )
    db.engine.execute(GanttJobsResource.__table__.insert(), mld_id_rid_s )

    db.flush()

    #"INSERT INTO  gantt_jobs_predictions  (moldable_job_id,start_time) VALUES "^
    #"INSERT INTO  gantt_jobs_resources (moldable_job_id,resource_id) VALUES "^

def get_current_jobs_dependencies():
# retrieve jobs dependencies *)
# return an hashtable, key = job_id, value = list of required jobs *)
    jobs_dependencies = {}

    req = db.query(JobDependencie, Job.state, Job.exit_code)\
            .filter(JobDependencie.index == "CURRENT")\
            .filter(Job.id == JobDependencie.job_id_required)\
            .all()

    for x in req:
        j_dep, state, exit_code = x
        if j_dep.job_id not in jobs_dependencies:
            jobs_dependencies[j_dep.job_id] = []
        jobs_dependencies[j_dep.job_id].append( (j_dep.job_id_required, state, exit_code) )

    return jobs_dependencies
