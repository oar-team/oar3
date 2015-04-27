import os

from oar.lib import (db, Job, MoldableJobDescription, JobResourceDescription,
                     JobResourceGroup, Resource, GanttJobsPrediction,
                     JobDependencie, GanttJobsResource, JobType,
                     JobStateLog, AssignedResource, FragJob, get_logger)
from oar.kao.utils import (notify_almighty, get_date, notify_user,
                           update_current_scheduler_priority, add_new_event)

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
                  [                  # first moldable instance
                     (1, 60,         # moldable id, walltime
                      # list of requests composed of
                      [  ( [("node", 2)], [(1,32)] ) ]
                  )                  # list of hierarchy request and filtered
                 ]                   # resources (Properties)

'''

global NO_PLACEHOLDER
global SET_PLACEHOLDER
global USE_PLACEHOLDER

NO_PLACEHOLDER = 0
SET_PLACEHOLDER = 1
USE_PLACEHOLDER = 2


class JobPseudo(object):

    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)


def get_waiting_jobs(queue, reservation='None'):
    # TODO  fairsharing_nb_job_limit
    waiting_jobs = {}
    waiting_jids = []
    nb_waiting_jobs = 0

    for j in db.query(Job).filter(Job.state == "Waiting")\
                          .filter(Job.queue_name == queue)\
                          .filter(Job.reservation == reservation):
        jid = int(j.id)
        waiting_jobs[jid] = j
        waiting_jids.append(jid)
        nb_waiting_jobs += 1

    return (waiting_jobs, waiting_jids, nb_waiting_jobs)


def get_jobs_types(jids, jobs):
    jobs_types = {}
    for j_type in db.query(JobType).filter(JobType.job_id.in_(tuple(jids))):
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
            if len(t_v) == 2:
                v = t_v[1]
            else:
                v = ""
            if jid not in jobs_types:
                jobs_types[jid] = dict()

            # print t, v
            (jobs_types[jid])[t] = v

    for job in jobs.itervalues():
        if job.id in jobs_types:
            job.types = jobs_types[job.id]
        else:
            job.types = {}


def set_jobs_cache_keys(jobs):
    """
    Set keys for job use by slot_set cache to speed up the search of suitable
    slots.

    Jobs with timesharing, placeholder or dependencies requirements are not
    suitable for this cache feature. Jobs in container might leverage of cache
    because container is link to a particular slot_set.

    For jobs with dependencies, they do not update the cache entries.

    """
    for job_id, job in jobs.iteritems():
        if (not job.ts) and (job.ph == NO_PLACEHOLDER):
            for res_rqt in job.mld_res_rqts:
                (mld_id, walltime, hy_res_rqts) = res_rqt
                job.key_cache[int(mld_id)] = str(walltime) + str(hy_res_rqts)


def get_data_jobs(jobs, jids, resource_set, job_security_time,
                  besteffort_duration=0):
    """
    oarsub -q test \
        -l "nodes=1+{network_address='node3'}/nodes=1/resource_id=1" \
        sleep

    job_id: 12 [(16L,
                 7200,
                 [
                     (
                         [(u'network_address', 1)],
                         [(0, 7)]
                     ),
                     (
                         [(u'network_address', 1), (u'resource_id', 1)],
                         [(4, 7)]
                     )
                 ])]

    """

    result = db.query(Job.id,
                      Job.properties,
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
        .filter(Job.id.in_(tuple(jids)))\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
        .filter(JobResourceDescription.group_id == JobResourceGroup.id)\
        .order_by(MoldableJobDescription.id,
                  JobResourceGroup.id,
                  JobResourceDescription.order)\
        .all()
    #            .join(MoldableJobDescription)\
    #            .join(JobResourceGroup)\
    #            .join(JobResourceDescription)\

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

    for x in result:
        # remove res_order
        (j_id, j_properties,
         mld_id,
         mld_id_walltime,
         jrg_id,
         jrg_mld_id,
         jrg_grp_property,
         res_jrg_id,
         res_type,
         res_value) = x
        # print  x
        #
        # new job
        #
        if j_id != prev_j_id:
            if first_job:
                first_job = False
            else:
                jrg.append((jr_descriptions, res_constraints))
                mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))
                job.mld_res_rqts = mld_res_rqts
                mld_res_rqts = []
                jrg = []
                jr_descriptions = []
                job.key_cache = {}
                job.deps = []
                job.ts = False
                job.ph = NO_PLACEHOLDER
                # print "======================"
                # print "job_id:",job.id,  job.mld_res_rqts
                # print "======================"

            prev_mld_id = mld_id
            prev_j_id = j_id
            job = jobs[j_id]
            if besteffort_duration:
                prev_mld_id_walltime = besteffort_duration
            else:
                prev_mld_id_walltime = mld_id_walltime + job_security_time

        else:
            #
            # new moldable_id
            #

            if mld_id != prev_mld_id:
                if jrg != []:
                    jrg.append((jr_descriptions, res_constraints))
                    mld_res_rqts.append(
                        (prev_mld_id, prev_mld_id_walltime, jrg))

                prev_mld_id = mld_id
                jrg = []
                jr_descriptions = []
                if besteffort_duration:
                    prev_mld_id_walltime = besteffort_duration
                else:
                    prev_mld_id_walltime = mld_id_walltime + job_security_time
        #
        # new job resources groupe_id
        #
        if jrg_id != prev_jrg_id:
            prev_jrg_id = jrg_id
            if jr_descriptions != []:
                jrg.append((jr_descriptions, res_constraints))
                jr_descriptions = []

        #
        # new set job descriptions
        #
        if res_jrg_id != prev_res_jrg_id:
            prev_res_jrg_id = res_jrg_id
            jr_descriptions = [(res_type, res_value)]

            #
            # determine resource constraints
            #
            if (j_properties == "" and (jrg_grp_property == "" or jrg_grp_property == "type = 'default'")):
                res_constraints = resource_set.roid_itvs
            else:
                if j_properties == "" or jrg_grp_property == "":
                    and_sql = ""
                else:
                    and_sql = " AND "

                sql_constraints = j_properties + and_sql + jrg_grp_property

                if sql_constraints in cache_constraints:
                    res_constraints = cache_constraints[sql_constraints]
                else:
                    request_constraints = db.query(
                        Resource.id).filter(sql_constraints).all()
                    roids = [resource_set.rid_i2o[int(y[0])]
                             for y in request_constraints]
                    res_constraints = unordered_ids2itvs(roids)
                    cache_constraints[sql_constraints] = res_constraints
        else:
            # add next res_type , res_value
            jr_descriptions.append((res_type, res_value))
            # print "@@@@@@@@@@@@@@@@@@@"
            # print jr_descriptions

    # complete the last job
    jrg.append((jr_descriptions, res_constraints))
    mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))

    job.mld_res_rqts = mld_res_rqts
    job.key_cache = {}
    job.deps = []
    job.ts = False
    job.ph = NO_PLACEHOLDER

    get_jobs_types(jids, jobs)
    get_current_jobs_dependencies(jobs)
    set_jobs_cache_keys(jobs)


def get_job_suspended_sum_duration(jid, now):

    suspended_duration = 0
    for j_state_log in db.query(JobStateLog).filter(JobStateLog.job_id == jid)\
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


# TODO available_suspended_res_itvs, now
def get_scheduled_jobs(resource_set, job_security_time, now):
    result = db.query(Job,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    jids = []
    jobs_lst = []
    jobs = {}
    prev_jid = 0
    roids = []

    global job

    # (job, a, b, c) = req[0]
    if result:
        for x in result:
            j, start_time, walltime, r_id = x
            # print x
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
            job.res_set = sub_intervals(
                job.res_set, resource_set.suspendable_roid_itvs)

        jobs_lst.append(job)
        jids.append(job.id)
        jobs[job.id] = job
        get_jobs_types(jids, jobs)

    return jobs_lst


def save_assigns(jobs, resource_set):
    # http://docs.sqlalchemy.org/en/rel_0_9/core/dml.html#sqlalchemy.sql.expression.Insert.values
    mld_id_start_time_s = []
    mld_id_rid_s = []
    for j in jobs.itervalues():
        mld_id_start_time_s.append(
            {'moldable_job_id': j.moldable_id, 'start_time': j.start_time})
        riods = itvs2ids(j.res_set)
        mld_id_rid_s.extend(
            [{'moldable_job_id': j.moldable_id,
              'resource_id': resource_set.rid_o2i[rid]} for rid in riods])

    log.info("save assignements")

    db.engine.execute(
        GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)
    db.engine.execute(GanttJobsResource.__table__.insert(), mld_id_rid_s)

    db.commit()

    # INSERT INTO  gantt_jobs_predictions  (moldable_job_id,start_time) VALUES
    # INSERT INTO  gantt_jobs_resources (moldable_job_id,resource_id) VALUES


def get_current_jobs_dependencies(jobs):
    # retrieve jobs dependencies *)
    # return an hashtable, key = job_id, value = list of required jobs *)

    req = db.query(JobDependencie, Job.state, Job.exit_code)\
            .filter(JobDependencie.index == "CURRENT")\
            .filter(Job.id == JobDependencie.job_id_required)\
            .all()

    for x in req:
        j_dep, state, exit_code = x
        if j_dep.job_id not in jobs:
            # These facts have no  particular impact
            log.warning(" during get dependencies for current job " +
                        str(job_id) + " is not in waiting state")
        else:
            jobs[j_dep.job_id].deps.append(
                (j_dep.job_id_required, state, exit_code))

# TO REMOVE ?
# TODO


def get_current_not_waiting_jobs_old():
    jobs = db.query(Job).filter(Job.state != "Waiting").all()
    current_not_waiting_jobs = {}
    jobids_by_state = {}
    for job in jobs:
        current_not_waiting_jobs[job.id] = job
        if job.state not in jobids_by_state:
            jobids_by_state[job.state] = []
        jobids_by_state[job.state].append[job.id]

    return (jobids_by_state, current_not_waiting_jobs)


def get_current_not_waiting_jobs():
    jobs = db.query(Job).filter(Job.state != "Waiting").all()
    jobs_by_state = {}
    for job in jobs:
        if job.state not in jobs_by_state:
            jobs_by_state[job.state] = []
        jobs_by_state[job.state].append(job)
    return (jobs_by_state)


def get_gantt_jobs_to_launch(current_time_sec):
    # NOTE1 does not use  m.moldable_index = \'CURRENT\' impacts Pg's performance
    # NOTE2 does not use   AND (resources.state IN (\'Dead\',\'Suspected\',\'Absent\')
    #                    OR resources.next_state IN (\'Dead\',\'Suspected\',\'Absent\'))
    # to reduce overhead

    req = db.query(Job, MoldableJobDescription.id)\
            .filter(GanttJobsPrediction.start_time <= current_time_sec)\
            .filter(Job.state == "Waiting")\
            .filter(Job.id == MoldableJobDescription.job_id)\
            .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
            .all()

    # TODO: verify
    #    $req = "SELECT DISTINCT(j.job_id)
    #            FROM gantt_jobs_resources g1, gantt_jobs_predictions g2,
    #                 jobs j, moldable_job_descriptions m, resources
    #            WHERE
    #               g1.moldable_job_id = g2.moldable_job_id
    #               AND m.moldable_id = g1.moldable_job_id
    #               AND j.job_id = m.moldable_job_id
    #               AND g2.start_time <= $date
    #               AND j.state = \'Waiting\'

    return req


def set_job_start_time_assigned_moldable_id(jid, start_time, moldable_id):
    # db.query(Job).update({Job.start_time:
    # start_time,Job.assigned_moldable_job: moldable_id}).filter(Job.id ==
    # jid)
    db.query(Job).filter(Job.id == jid).update(
        {Job.start_time: start_time, Job.assigned_moldable_job: moldable_id})
    db.commit()


def set_jobs_start_time(tuple_jids, start_time):
    db.query(Job).filter(Job.id.in_(tuple_jids)).update(
        {Job.start_time: start_time})
    db.commit()


def set_jobs_state(tuple_jids, state):  # NOT USED
    # TODO complete to enhance performance by vectorizing operations
    # db.query(Job).update({Job.state: state}).filter(Job.job_id.in_( tuple_jids ))
    # db.commit()
    pass


def set_job_state(jid, state):

    # TODO
    # TODO Later: notify_user
    # TODO Later: update_current_scheduler_priority

    result = db.query(Job).filter(Job.id == jid)\
                          .filter(Job.state != 'Error')\
                          .filter(Job.state != 'Terminated')\
                          .filter(Job.state != state)\
                          .update({Job.state: state})
    db.commit()

    if result == 1:
        log.debug(
            "Job state updated, job_id: " + str(jid) + ", wanted state: " + state)

        date = get_date()

        # TODO: optimize job log
        db.query(JobStateLog).filter(JobStateLog.date_stop == 0)\
                             .filter(JobStateLog.job_id == jid)\
                             .update({JobStateLog.date_stop: date})
        req = db.insert(JobStateLog).values(
            {'job_id': jid, 'job_state': state, 'date_start': date})
        db.engine.execute(req)
        db.commit()

        if state == "Terminated" or state == "Error" or state == "toLaunch" or \
           state == "Running" or state == "Suspended" or state == "Resuming":
            job = db.query(Job).filter(Job.id == jid).one()
            if state == "Suspend":
                notify_user(job, "SUSPENDED", "Job is suspended.")
            elif state == "Resuming":
                notify_user(job, "RESUMING", "Job is resuming.")
            elif state == "Running":
                notify_user(job, "RUNNING", "Job is running.")
            elif state == "toLaunch":
                update_current_scheduler_priority(job, "+2", "START")
            else:  # job is "Terminated" or ($state eq "Error")
                if job.stop_time < job.start_time:
                    db.query(Job).filter(Job.id == jid)\
                                 .update({Job.stop_time: job.start_time})
                    db.commit()

                if job.assigned_moldable_job != "0":
                    # Update last_job_date field for resources used
                    update_scheduler_last_job_date(
                        date, int(job.assigned_moldable_job))

                if state == "Terminated":
                    notify_user(job, "END", "Job stopped normally.")
                else:
                    # Verify if the job was suspended and if the resource
                    # property suspended is updated
                    if job.suspended == "YES":
                        r = get_current_resources_with_suspended_job()

                        if r != ():
                            db.query(Resource).filter(~Resource.id.in_(r))\
                                              .update({Resource.suspended_jobs: 'NO'})

                        else:
                            db.query(Resource).update(
                                {Resource.suspended_jobs: 'NO'})
                        db.commit()

                    notify_user(
                        job, "ERROR", "Job stopped abnormally or an OAR error occured.")

                update_current_scheduler_priority(job, "-2", "STOP")

                # Here we must not be asynchronously with the scheduler
                log_job(job)
                # $dbh is valid so these 2 variables must be defined
                nb_sent = notify_almighty("ChState")
                if nb_sent == 0:
                    log.warn("Not able to notify almighty to launch the job " +
                             str(job.id) + " (socket error)")

    else:
        log.warning("Job is already termindated or in error or wanted state, job_id: " +
                    str(jid) + ", wanted state: " + state)

# NO USED


def add_resource_jobs_pairs(tuple_mld_ids):
    resources_mld_ids = db.query(GanttJobsResource)\
                          .filter(GanttJobsResource.job_id.in_(tuple_mld_ids))\
                          .all()

    assigned_resources = [{'moldable_job_id': res_mld_id.moldable_id,
                           'resource_id': res_mld_id.resource_id} for res_mld_id in resources_mld_ids]

    db.engine.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()


def add_resource_job_pairs(moldable_id):
    resources_mld_ids = db.query(GanttJobsResource)\
                          .filter(GanttJobsResource.moldable_id == moldable_id)\
                          .all()

    assigned_resources = [{'moldable_job_id': res_mld_id.moldable_id,
                           'resource_id': res_mld_id.resource_id} for res_mld_id in resources_mld_ids]

    db.engine.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()


# Return the list of resources where there are Suspended jobs
# args: base
def get_current_resources_with_suspended_job():
    res = db.query(AssignedResource.resource_id).filter(AssignedResource.index == 'CURRENT')\
                                                .filter(Job.state == 'Suspended')\
                                                .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
                                                .all()

    return tuple(r for r in res)

# log_job
# sets the index fields to LOG on several tables
# this will speed up future queries
# parameters : base, jobid
# return value : /


def log_job(job):
    db.query(MoldableJobDescription)\
      .filter(MoldableJobDescription.index == 'CURRENT')\
      .filter(MoldableJobDescription.job_id == job.id)\
      .update({MoldableJobDescription.index: 'LOG'}, synchronize_session=False)

    db.query(JobResourceDescription)\
      .filter(MoldableJobDescription.job_id == job.id)\
      .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
      .filter(JobResourceDescription.group_id == JobResourceGroup.id) \
      .update({JobResourceDescription.index: 'LOG'}, synchronize_session=False)

    db.query(JobResourceGroup)\
      .filter(JobResourceGroup.index == 'CURRENT')\
      .filter(MoldableJobDescription.index == 'LOG')\
      .filter(MoldableJobDescription.job_id == job.id)\
      .filter(JobResourceGroup.moldable_id == MoldableJobDescription.id)\
      .update({JobResourceGroup.index: 'LOG'}, synchronize_session=False)

    db.query(JobType)\
      .filter(JobType.types_index == 'CURRENT')\
      .filter(JobType.job_id == job.id)\
      .update({JobType.types_index: 'LOG'}, synchronize_session=False)

    db.query(JobDependencie)\
      .filter(JobDependencie.index == 'CURRENT')\
      .filter(JobDependencie.job_id == job.id)\
      .update({JobDependencie.index: 'LOG'}, synchronize_session=False)

    if job.assigned_moldable_job != "0":
        db.query(AssignedResource)\
          .filter(AssignedResource.assigned_resource_index == 'CURRENT')\
          .filter(AssignedResource.d == int(job.assigned_moldable_job))\
          .update({AssignedResource.assigned_resource_index: 'LOG'},
                  synchronize_session=False)
    db.commit()


def get_gantt_waiting_interactive_prediction_date():
    req = db.query(Job.id,
                   Job.info_type,
                   GanttJobsPrediction.start_time,
                   Job.message)\
            .filter(Job.state == 'Waiting')\
            .filter(Job.type == 'INTERACTIVE')\
            .filter(Job.reservation == 'None')\
            .filter(MoldableJobDescription.job_id == Job.id)\
            .filter(GanttJobsPrediction.moldable_id == MoldableJobDescription.id)\
            .all()
    return req


def insert_job(**kwargs):
    """ Insert job in database

    #   "{ sql1 }/prop1=1/prop2=3+{sql2}/prop3=2/prop4=1/prop5=1+...,walltime=60"
    #
    #   res = "/switch=2/nodes=10+{lic_type = 'mathlab'}/licence=20" type="besteffort, container"
    #
    insert_job(
    res = [
        ( 60, [("switch=2/nodes=20", ""), ("licence=20", "lic_type = 'mathlab'")] ) ],
    type = "besteffort, container",
    job_user= "")


    """
    kwargs['launching_directory'] = ""
    kwargs['checkpoint_signal'] = 0

    if 'res' in kwargs:
        res = kwargs.pop('res')
    else:
        res = [(60, [('resource_id=1', "")])]

    if 'types' in kwargs:
        types = kwargs.pop('types')
    else:
        types = ""

    if 'queue_name' not in kwargs:
        kwargs['queue_name'] = 'default'

    ins = Job.__table__.insert().values(**kwargs)
    result = db.engine.execute(ins)
    job_id = result.inserted_primary_key[0]

    mld_jid_walltimes = []
    res_grps = []

    for res_mld in res:
        w, res_grp = res_mld
        mld_jid_walltimes.append(
            {'moldable_job_id': job_id, 'moldable_walltime': w})
        res_grps.append(res_grp)

    result = db.engine.execute(MoldableJobDescription.__table__.insert(),
                               mld_jid_walltimes)

    if len(mld_jid_walltimes) == 1:
        mld_ids = [result.inserted_primary_key[0]]
    else:
        r = db.query(MoldableJobDescription.id)\
              .filter(MoldableJobDescription.job_id == job_id).all()
        mld_ids = [x for e in r for x in e]

    for mld_idx, res_grp in enumerate(res_grps):
        # job_resource_groups
        mld_id_property = []
        res_hys = []

        moldable_id = mld_ids[mld_idx]

        for r_hy_prop in res_grp:
            (res_hy, properties) = r_hy_prop
            mld_id_property.append({'res_group_moldable_id': moldable_id,
                                    'res_group_property': properties})
            res_hys.append(res_hy)

        # print "mld_id_property: ", mld_id_property
        result = db.engine.execute(JobResourceGroup.__table__.insert(),
                                   mld_id_property)

        if len(mld_id_property) == 1:
            grp_ids = [result.inserted_primary_key[0]]
        else:
            r = db.query(JobResourceGroup.id)\
                  .filter(JobResourceGroup.moldable_id == moldable_id).all()
            grp_ids = [x for e in r for x in e]

        # job_resource_descriptions
        for grp_idx, res_hy in enumerate(res_hys):
            res_description = []
            for idx, val in enumerate(res_hy.split('/')):
                tv = val.split('=')
                res_description.append({'res_job_group_id': grp_ids[grp_idx],
                                        'res_job_resource_type': tv[0],
                                        'res_job_value': tv[1],
                                        'res_job_order': idx})

            db.engine.execute(JobResourceDescription.__table__.insert(),
                              res_description)

    if types:
        ins = [{'job_id': job_id, 'type': typ} for typ in types.split(',')]
        db.engine.execute(JobResourceDescription.__table__.insert(), ins)

    return job_id


def get_job(job_id):
    try:
        job = db.query(Job).filter(Job.id == job_id).one()
    except Exception, e:
        log.warn("get_job(" + str(job_id) + ") raises execption: " + str(e))
        return None
    else:
        return job
# frag_job
# sets the flag 'ToFrag' of a job to 'Yes'
# parameters : base, jobid
# return value : 0 on success, -1 on error (if the user calling this method
#                is not the user running the job or oar), -2 if the job was
#                already killed
# side effects : changes the field ToFrag of the job in the table Jobs


def frag_job(jid):

    if 'OARDO_USER' in os.environ:
        luser = os.environ['OARDO_USER']
    else:
        luser = os.environ['USER']

    job = get_job(jid)

    if (job is not None) and ((luser == job.user)
                              or (luser == 'oar')
                              or (luser == 'root')):
        res = db.query(FragJob).filter(FragJob.job_id == jid).all()
        if len(res) == 0:
            date = get_date()
            frajob = FragJob(job_id=jid, date=date)
            db.add(frajob)
            db.commit()
            add_new_event("FRAG_JOB_REQUEST",
                          jid, "User %s requested to frag the job %s"
                          % (luser, str(jid)))
            return 0
        else:
            # Job already killed
            return -2
    else:
        return -1


def set_job_resa_state(job_id, state):
    ''' sets the reservation field of the job of id passed in parameter
    parameters : base, jobid, state
    return value : None
    side effects : changes the field state of the job in the table Jobs
    '''
    db.query(Job).filter(Job.id == job_id).update({Job.reservation: state})
    db.commit()


def set_job_message(job_id, message):
    db.query(Job).filter(Job.id == job_id).update({Job.message: message})
    db.commit()


def get_waiting_reservation_jobs_specific_queue(queue_name):
    '''Get all waiting reservation jobs in the specified queue
    parameter : database ref, queuename
    return an array of job informations
    '''
    waiting_scheduled_ar_jobs = db.query(Job)\
                                  .filter((Job.state == 'Waiting')
                                          |
                                          (Job.state == 'toAckReservation'))\
                                  .filter(Job.reservation == 'Scheduled')\
                                  .filter(Job.queue_name == queue_name)\
                                  .order_by(Job.id).all()
    return waiting_scheduled_ar_jobs


def update_scheduler_last_job_date(date, moldable_id):
    ''' used to allow search_idle_nodes to operate for dynamic node management feature (Hulot)
    '''
    db.query(Resource).filter(AssignedResource.moldable_id == moldable_id)\
                      .filter(Resource.id == AssignedResource.resource_id)\
                      .update({Resource.last_job_date: date}, synchronize_session=False)
    db.commit()


# Get all waiting reservation jobs
# parameter : database ref
# return an array of moldable job informations
def get_waiting_reservations_already_scheduled(resource_set, job_security_time):

    result = db.query(Job,
                      GanttJobsPrediction.start_time, GanttJobsResource.resource_id,
                      MoldableJobDescription.walltime, MoldableJobDescription.id)\
        .filter((Job.state == 'Waiting') | (Job.state == 'toAckReservation'))\
        .filter(Job.reservation == 'Scheduled')\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .filter(GanttJobsPrediction.moldable_id == MoldableJobDescription.id)\
        .filter(GanttJobsResource.moldable_id == MoldableJobDescription.id)\
        .all()

    first_job = True
    jobs = {}
    jids = []

    prev_jid = 0
    roids = []

    global job

    for x in result:
        j, start_time, resource_id, walltime, moldable_id = x

        if j.id != prev_jid:
            if first_job:
                first_job = False
            else:
                job.res_set = unordered_ids2itvs(roids)
                jids.append(job.id)
                jobs[job.id] = job
                roids = []

            prev_jid = j.id
            job = j
            job.start_time = start_time
            job.walltime = walltime + job_security_time

        roids.append(resource_set.rid_i2o[r_id])

    job.res_set = unordered_ids2itvs(roids)

    jids.append(job.id)
    jobs[job.id] = job
    get_jobs_types(jids, jobs)

    return (jids, jobs)


def gantt_flush_tables(reservations_to_keep_mld_ids):
    '''Flush gantt tables but keep accepted advance reservations'''

    if reservations_to_keep_mld_ids != []:
        db.query(GanttJobsPrediction)\
          .filter(~GanttJobsPrediction.moldable_id.in_(tuple(reservations_to_keep_mld_ids)))\
          .delete()
        db.query(GanttJobsResource)\
          .filter(~GanttJobsResource.moldable_id.in_(tuple(reservations_to_keep_mld_ids)))\
          .delete()
    else:
        db.query(GanttJobsPrediction).delete()
        db.query(GanttJobsResource).delete()

    db.commit()
