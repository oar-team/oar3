# coding: utf-8
from __future__ import unicode_literals, print_function

import os
from copy import deepcopy

from sqlalchemy import distinct
from sqlalchemy import text
from sqlalchemy.orm import aliased

from oar.lib import (db, Job, MoldableJobDescription, JobResourceDescription,
                     JobResourceGroup, Resource, GanttJobsPrediction,
                     JobDependencie, GanttJobsResource, JobType,
                     JobStateLog, AssignedResource, FragJob,
                     get_logger, config)
from oar.lib.psycopg2 import pg_bulk_insert
from oar.lib.compat import iteritems, itervalues

from oar.lib.tools import (update_current_scheduler_priority, add_new_event)
import oar.lib.tools as tools
from oar.lib.interval import unordered_ids2itvs, itvs2ids, sub_intervals


from oar.kao.helpers import extract_find_assign_args


logger = get_logger("oar.kamelot")

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
                      [  ( [("node", 2), ("resource_id", 4)], [(1,32)] ) ]
                  )                  # list of hierarchy request and filtered
                 ]                   # resources (Properties)

'''

NO_PLACEHOLDER = 0
PLACEHOLDER = 1
ALLOW = 2


class JobPseudo(object):
    ''' Define a simple job class without database counter part
    '''
    types = {}
    deps = []
    key_cache = {}
    ts = False
    ph = 0
    assign = False
    assign_args = ()
    assign_kwargs = {}
    find = False
    find_args = ()
    find_kwargs = {}
    queue_name = 'default'
    user = ''
    project = ''

    def __init__(self, **kwargs):
        self.mld_res_rqts = []
        for key, value in iteritems(kwargs):
            setattr(self, key, value)

    def simple_req(self, resources_req, walltime, resources_constraint):
        ''' Allow declaration of simple resources request.
        Examples:
        - j.simple_req(('node', 2), 60, [(1, 32)])
        - j.simple_req([('node', 2), ('core', 4)], 600, [(1, 64)])
        '''
        if type(resources_req) == tuple:
            res_req = [resources_req]
        else:
            res_req = resources_req
        self.mld_res_rqts = [(1, walltime, [(res_req, deepcopy(resources_constraint))])]


def get_waiting_jobs(queue, reservation='None'):
    # TODO  fairsharing_nb_job_limit
    waiting_jobs = {}
    waiting_jids = []
    nb_waiting_jobs = 0

    for j in db.query(Job).filter(Job.state == "Waiting")\
                          .filter(Job.queue_name == queue)\
                          .filter(Job.reservation == reservation)\
                          .order_by(Job.id).all():
        jid = int(j.id)
        waiting_jobs[jid] = j
        waiting_jids.append(jid)
        nb_waiting_jobs += 1

    return (waiting_jobs, waiting_jids, nb_waiting_jobs)


def get_jobs_types(jids, jobs):
    import oar.kao.advanced_scheduling
    jobs_types = {}
    for j_type in db.query(JobType).filter(JobType.job_id.in_(tuple(jids))):
        jid = j_type.job_id
        job = jobs[jid]
        t_v = j_type.type.split("=")
        t = t_v[0]
        if t == "timesharing":
            job.ts = True
            job.ts_user, job.ts_name = t_v[1].split(',')
        elif t == "placeholder":
            job.ph = PLACEHOLDER
            job.ph_name = t_v[1]
        elif t == "allow":
            job.ph = ALLOW
            job.ph_name = t_v[1]
        elif t == "assign":
            job.assign = True
            raw_args = '='.join(t_v[1:])
            funcname, job.assign_args, job.assign_kwargs = extract_find_assign_args(raw_args)
            job.assign_func = getattr(oar.kao.advanced_scheduling, 'assign_%s' % funcname)
        elif t == "find":
            job.find = True
            raw_args = '='.join(t_v[1:])
            funcname, job.find_args, job.find_kwargs = extract_find_assign_args(raw_args)
            job.find_func = getattr(oar.kao.advanced_scheduling, 'find_%s' % funcname)
        else:
            if len(t_v) == 2:
                v = t_v[1]
            else:
                v = ""
            if jid not in jobs_types:
                jobs_types[jid] = dict()

            (jobs_types[jid])[t] = v

    for job in itervalues(jobs):
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
    for job_id, job in iteritems(jobs):
        if (not job.ts) and (job.ph == NO_PLACEHOLDER):
            for res_rqt in job.mld_res_rqts:
                (moldable_id, walltime, hy_res_rqts) = res_rqt
                job.key_cache[int(moldable_id)] = str(walltime) + str(hy_res_rqts)


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
         moldable_id,
         mld_id_walltime,
         jrg_id,
         jrg_grp_property,
         res_jrg_id,
         res_type,
         res_value) = x
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
                job.assign = False
                job.find = False

            prev_mld_id = moldable_id
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

            if moldable_id != prev_mld_id:
                if jrg != []:
                    jrg.append((jr_descriptions, res_constraints))
                    mld_res_rqts.append(
                        (prev_mld_id, prev_mld_id_walltime, jrg))

                prev_mld_id = moldable_id
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
                res_constraints = deepcopy(resource_set.roid_itvs)
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
                        Resource.id).filter(text(sql_constraints)).all()
                    roids = [resource_set.rid_i2o[int(y[0])]
                             for y in request_constraints]
                    res_constraints = unordered_ids2itvs(roids)
                    cache_constraints[sql_constraints] = res_constraints
        else:
            # add next res_type , res_value
            jr_descriptions.append((res_type, res_value))

    # complete the last job
    jrg.append((jr_descriptions, res_constraints))
    mld_res_rqts.append((prev_mld_id, prev_mld_id_walltime, jrg))

    job.mld_res_rqts = mld_res_rqts
    job.key_cache = {}
    job.deps = []
    job.ts = False
    job.ph = NO_PLACEHOLDER
    job.assign = False
    job.find = False

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
def extract_scheduled_jobs(result, resource_set, job_security_time, now):

    jids = []
    jobs_lst = []
    jobs = {}
    prev_jid = 0
    roids = []
    rid2jid = {}

    global job

    # (job, a, b, c) = req[0]
    if result:
        for x in result:
            j, moldable_id, start_time, walltime, r_id = x
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
                job.moldable_id = moldable_id
                job.ts = False
                job.ph = NO_PLACEHOLDER
                job.assign = False
                job.find = False
                if job.suspended == "YES":
                    job.walltime += get_job_suspended_sum_duration(job.id, now)

            roid = resource_set.rid_i2o[r_id]
            roids.append(roid)
            rid2jid[roid] = j.id

        job.res_set = unordered_ids2itvs(roids)
        if job.state == "Suspended":
            job.res_set = sub_intervals(
                job.res_set, resource_set.suspendable_roid_itvs)

        jobs_lst.append(job)
        jids.append(job.id)
        jobs[job.id] = job
        get_jobs_types(jids, jobs)

    return (jobs, jobs_lst, jids, rid2jid)


# TODO available_suspended_res_itvs, now
def get_scheduled_jobs(resource_set, job_security_time, now):
    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    jobs, jobs_lst, jids, rid2jid = extract_scheduled_jobs(result, resource_set,
                                                           job_security_time, now)

    return jobs_lst


def get_after_sched_no_AR_jobs(queue_name, resource_set, job_security_time, now):
    """ Get waiting jobs which are not AR and after scheduler round
    """
    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(Job.queue_name == queue_name)\
        .filter(Job.state == 'Waiting')\
        .filter(Job.reservation == 'None')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    _, jobs_lst, _, _ = extract_scheduled_jobs(result, resource_set,
                                               job_security_time, now)

    return jobs_lst


def get_waiting_scheduled_AR_jobs(queue_name, resource_set, job_security_time, now):
    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(MoldableJobDescription.index == 'CURRENT')\
        .filter(Job.queue_name == queue_name)\
        .filter(Job.reservation == 'Scheduled')\
        .filter(Job.state == 'Waiting')\
        .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .order_by(Job.start_time, Job.id)\
        .all()

    _, jobs_lst, _, _ = extract_scheduled_jobs(result, resource_set,
                                               job_security_time, now)

    return jobs_lst


def get_gantt_jobs_to_launch(resource_set, job_security_time, now):

    # get unlaunchable jobs
    # NOT USED launcher will manage these cases ??? (MUST BE CONFIRMED)
    #
    #result = db.query(distinct(Job.id))\
    #           .filter(GanttJobsPrediction.start_time <= now)\
    #           .filter(Job.state == "Waiting")\
    #           .filter(Job.id == MoldableJobDescription.job_id)\
    #           .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
    #           .filter(GanttJobsResource.moldable_id == GanttJobsPrediction.moldable_id)\
    # AND (resources.state IN (\'Dead\',\'Suspected\',\'Absent\')
    #                   OR resources.next_state IN (\'Dead\',\'Suspected\',\'Absent\'))
    #
    #           .all()

    result = db.query(Job,
                      GanttJobsPrediction.moldable_id,
                      GanttJobsPrediction.start_time,
                      MoldableJobDescription.walltime,
                      GanttJobsResource.resource_id)\
        .filter(GanttJobsPrediction.start_time <= now)\
        .filter(Job.state == "Waiting")\
        .filter(Job.id == MoldableJobDescription.job_id)\
        .filter(MoldableJobDescription.id == GanttJobsPrediction.moldable_id)\
        .filter(GanttJobsResource.moldable_id
                == GanttJobsPrediction.moldable_id)\
        .filter(Resource.id == GanttJobsResource.resource_id)\
        .filter(Resource.state == 'Alive')\
        .all()

    jobs, jobs_lst, _, rid2jid = extract_scheduled_jobs(result, resource_set,
                                                        job_security_time, now)

    return (jobs, jobs_lst, rid2jid)


def save_assigns(jobs, resource_set):
    # http://docs.sqlalchemy.org/en/rel_0_9/core/dml.html#sqlalchemy.sql.expression.Insert.values
    if len(jobs) > 0:
        logger.debug("nb job to save: " + str(len(jobs)))
        mld_id_start_time_s = []
        mld_id_rid_s = []
        for j in itervalues(jobs):
            if j.start_time > -1:
                logger.debug("job_id to save: " + str(j.id))
                mld_id_start_time_s.append(
                    {'moldable_job_id': j.moldable_id, 'start_time': j.start_time})
                riods = itvs2ids(j.res_set)
                mld_id_rid_s.extend(
                    [{'moldable_job_id': j.moldable_id,
                      'resource_id': resource_set.rid_o2i[rid]} for rid in riods])

        logger.info("save assignements")

        db.session.execute(
            GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)
        db.session.execute(GanttJobsResource.__table__.insert(), mld_id_rid_s)
        db.commit()

    # INSERT INTO  gantt_jobs_predictions  (moldable_job_id,start_time) VALUES
    # INSERT INTO  gantt_jobs_resources (moldable_job_id,resource_id) VALUES


def save_assigns_bulk(jobs, resource_set):

    if len(jobs) > 0:
        logger.debug("nb job to save: " + str(len(jobs)))
        mld_id_start_time_s = []
        mld_id_rid_s = []
        for j in itervalues(jobs):
            if j.start_time > -1:
                logger.debug("job_id  to save: " + str(j.id))
                mld_id_start_time_s.append((j.moldable_id, j.start_time))
                riods = itvs2ids(j.res_set)
                mld_id_rid_s.extend(
                    [(j.moldable_id, resource_set.rid_o2i[rid]) for rid in riods])

        logger.info("save assignements")

        with db.engine.connect() as to_conn:
            cursor = to_conn.connection.cursor()
            pg_bulk_insert(cursor, db['gantt_jobs_predictions'], mld_id_start_time_s,
                           ('moldable_job_id', 'start_time'), binary=True)
            pg_bulk_insert(cursor, db['queues'], mld_id_rid_s,
                           ('moldable_job_id', 'resource_id'), binary=True)


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
            # This fact have no particular impact
            logger.warning(" during get dependencies for current job %s is not in waiting state" % str(j_dep.job_id))
        else:
            jobs[j_dep.job_id].deps.append(
                (j_dep.job_id_required, state, exit_code))


def get_current_not_waiting_jobs():
    jobs = db.query(Job).filter(Job.state != "Waiting").all()
    jobs_by_state = {}
    for job in jobs:
        if job.state not in jobs_by_state:
            jobs_by_state[job.state] = []
        jobs_by_state[job.state].append(job)
    return (jobs_by_state)


def set_job_start_time_assigned_moldable_id(jid, start_time, moldable_id):
    # db.query(Job).update({Job.start_time:
    # start_time,Job.assigned_moldable_job: moldable_id}).filter(Job.id ==
    # jid)
    db.query(Job).filter(Job.id == jid).update(
        {Job.start_time: start_time, Job.assigned_moldable_job: moldable_id})
    db.commit()


def set_jobs_start_time(tuple_jids, start_time):

    db.query(Job).filter(Job.id.in_(tuple_jids)).update(
        {Job.start_time: start_time}, synchronize_session=False)
    db.commit()


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

    if result == 1:  # OK for sqlite
        logger.debug(
            "Job state updated, job_id: " + str(jid) + ", wanted state: " + state)

        date = tools.get_date()

        # TODO: optimize job log
        db.query(JobStateLog).filter(JobStateLog.date_stop == 0)\
                             .filter(JobStateLog.job_id == jid)\
                             .update({JobStateLog.date_stop: date})
        db.commit()
        req = db.insert(JobStateLog).values(
            {'job_id': jid, 'job_state': state, 'date_start': date})
        db.session.execute(req)

        if state == "Terminated" or state == "Error" or state == "toLaunch" or \
           state == "Running" or state == "Suspended" or state == "Resuming":
            job = db.query(Job).filter(Job.id == jid).one()
            if state == "Suspend":
                tools.notify_user(job, "SUSPENDED", "Job is suspended.")
            elif state == "Resuming":
                tools.notify_user(job, "RESUMING", "Job is resuming.")
            elif state == "Running":
                tools.notify_user(job, "RUNNING", "Job is running.")
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
                    tools.notify_user(job, "END", "Job stopped normally.")
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

                    tools.notify_user(
                        job, "ERROR", "Job stopped abnormally or an OAR error occured.")

                update_current_scheduler_priority(job, "-2", "STOP")

                # Here we must not be asynchronously with the scheduler
                log_job(job)
                # $dbh is valid so these 2 variables must be defined
                nb_sent = tools.notify_almighty("ChState")
                if nb_sent == 0:
                    logger.warning("Not able to notify almighty to launch the job " +
                                   str(job.id) + " (socket error)")

    else:
        logger.warning("Job is already termindated or in error or wanted state, job_id: " +
                       str(jid) + ", wanted state: " + state)

# NO USED


def add_resource_jobs_pairs(tuple_mld_ids):  # pragma: no cover
    resources_mld_ids = db.query(GanttJobsResource)\
                          .filter(GanttJobsResource.job_id.in_(tuple_mld_ids))\
                          .all()

    assigned_resources = [{'moldable_job_id': res_mld_id.moldable_id,
                           'resource_id': res_mld_id.resource_id} for res_mld_id in resources_mld_ids]

    db.session.execute(AssignedResource.__table__.insert(), assigned_resources)
    db.commit()

def add_resource_job_pairs(moldable_id):
    resources_mld_ids = db.query(GanttJobsResource)\
                          .filter(GanttJobsResource.moldable_id == moldable_id)\
                          .all()

    assigned_resources = [{'moldable_job_id': res_mld_id.moldable_id,
                           'resource_id': res_mld_id.resource_id} for res_mld_id in resources_mld_ids]

    db.session.execute(AssignedResource.__table__.insert(), assigned_resources)
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


def log_job(job):  # pragma: no cover
    if db.dialect == "sqlite":
        return
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
          .filter(AssignedResource.index == 'CURRENT')\
          .filter(AssignedResource.moldable_id == int(job.assigned_moldable_job))\
          .update({AssignedResource.index: 'LOG'},
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
    #   res = "/switch=2/nodes=10+{lic_type = 'mathlab'}/licence=20" types="besteffort, container"
    #
    insert_job(
    res = [
        ( 60, [("switch=2/nodes=20", ""), ("licence=20", "lic_type = 'mathlab'")] ) ],
    types = ["besteffort", "container"],
    user= "")


    """

    default_values = {'launching_directory': "", 'checkpoint_signal': 0, 'properties': ""}

    for k, v in iteritems(default_values):
        if k not in kwargs:
            kwargs[k] = v

    if 'res' in kwargs:
        res = kwargs.pop('res')
    else:
        res = [(60, [('resource_id=1', "")])]

    if 'types' in kwargs:
        types = kwargs.pop('types')
    else:
        types = []

    if 'queue_name' not in kwargs:
        kwargs['queue_name'] = 'default'

    if 'user' in kwargs:
        kwargs['job_user'] = kwargs.pop('user')

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    job_id = result.inserted_primary_key[0]

    mld_jid_walltimes = []
    res_grps = []

    for res_mld in res:
        w, res_grp = res_mld
        mld_jid_walltimes.append(
            {'moldable_job_id': job_id, 'moldable_walltime': w})
        res_grps.append(res_grp)

    result = db.session.execute(MoldableJobDescription.__table__.insert(),
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

        result = db.session.execute(JobResourceGroup.__table__.insert(),
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

            db.session.execute(JobResourceDescription.__table__.insert(),
                               res_description)

    if types:
        ins = [{'job_id': job_id, 'type': typ} for typ in types]
        db.session.execute(JobType.__table__.insert(), ins)

    return job_id


def get_job(job_id):  # pragma: no cover
    try:
        job = db.query(Job).filter(Job.id == job_id).one()
    except Exception as e:
        logger.warning("get_job(" + str(job_id) + ") raises execption: " + str(e))
        return None
    else:
        return job


# frag_job
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

            date = tools.get_date()
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

    if db.dialect == "sqlite":
        subquery = db.query(AssignedResource.resource_id).filter_by(moldable_id=moldable_id)\
                     .subquery()
        db.query(Resource).filter(Resource.id.in_(subquery))\
                          .update({Resource.last_job_date: date}, synchronize_session=False)

    else:
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
        .order_by(Job.id).all()

    first_job = True
    jobs = {}
    jids = []

    prev_jid = 0
    roids = []

    global job
    if result:
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

            roids.append(resource_set.rid_i2o[resource_id])

        job.res_set = unordered_ids2itvs(roids)

        jids.append(job.id)
        jobs[job.id] = job

        get_jobs_types(jids, jobs)

    return (jids, jobs)


def gantt_flush_tables(reservations_to_keep_mld_ids):
    '''Flush gantt tables but keep accepted advance reservations'''

    if reservations_to_keep_mld_ids != []:
        logger.debug("reservations_to_keep_mld_ids[0]: " + str(reservations_to_keep_mld_ids[0]))
        db.query(GanttJobsPrediction)\
          .filter(~GanttJobsPrediction.moldable_id.in_(tuple(reservations_to_keep_mld_ids)))\
          .delete(synchronize_session=False)
        db.query(GanttJobsResource)\
          .filter(~GanttJobsResource.moldable_id.in_(tuple(reservations_to_keep_mld_ids)))\
          .delete(synchronize_session=False)
    else:
        db.query(GanttJobsPrediction).delete()
        db.query(GanttJobsResource).delete()

    db.commit()


def get_jobs_in_multiple_states(states, resource_set):

    result = db.query(Job, AssignedResource.moldable_id, AssignedResource.resource_id)\
               .filter(Job.state.in_(tuple(states)))\
               .filter(Job.assigned_moldable_job == AssignedResource.moldable_id)\
               .order_by(Job.id).all()

    first_job = True
    jobs = {}

    prev_jid = 0
    roids = []

    global job

    if result:
        for x in result:
            j, moldable_id, resource_id = x

            if j.id != prev_jid:
                if first_job:
                    first_job = False
                else:
                    job.res_set = unordered_ids2itvs(roids)
                    jobs[job.id] = job
                    roids = []

                prev_jid = j.id
                job = j
                job.moldable_id = moldable_id

            roids.append(resource_set.rid_i2o[resource_id])

        job.res_set = unordered_ids2itvs(roids)
        jobs[job.id] = job

    return jobs


def get_jobs_ids_in_multiple_states(states):

    result = db.query(Job.id, Job.states)\
               .filter(Job.state.in_(tuple(states)))\
               .order_by(Job.id).all()

    jids_states = {}

    if result:
        for x in result:
            jid, state = x
            jids_states[jid] = state
    return jids_states


# set walltime for a moldable job
def set_moldable_job_max_time(moldable_id, walltime):

    db.query(MoldableJobDescription)\
      .filter(MoldableJobDescription.id == moldable_id)\
      .update({MoldableJobDescription.walltime: walltime})

    db.commit()


# Update start_time in gantt for a specified job
def set_gantt_job_start_time(moldable_id, current_time_sec):

    db.query(GanttJobsPrediction)\
      .filter(GanttJobsPrediction.moldable_id == moldable_id)\
      .update({GanttJobsPrediction.start_time: current_time_sec})

    db.commit()


def remove_gantt_resource_job(moldable_id, job_res_set, resource_set):

    riods = itvs2ids(job_res_set)
    resource_ids = [resource_set.rid_o2i[rid] for rid in riods]

    db.query(GanttJobsResource)\
      .filter(GanttJobsResource.moldable_id == moldable_id)\
      .filter(~GanttJobsResource.resource_id.in_(tuple(resource_ids)))\
      .delete(synchronize_session=False)

    db.commit()


def is_timesharing_for_two_jobs(j1, j2):
    if ('timesharing' in j1.types) and ('timesharing' in j2.types):
        t1 = j1.types['timesharing']
        t2 = j2.types['timesharing']

        if (t1 == '*,*') and (t2 == '*,*'):
            return True

        if (j1.user == j2.user) and (j1.name == j2.name):
            return True

        if (j1.user == j2.user) and (t1 == 'user,*') and (t2 == 'user,*'):
            return True

        if (j1.name == j2.name) and (t1 == '*,name') and (t1 == '*,name'):
            return True

    return False


def get_jobs_on_resuming_job_resources(job_id):
    '''Return the list of jobs running on resources allocated to another given job'''
    j1 = aliased(Job)
    j2 = aliased(Job)
    a1 = aliased(AssignedResource)
    a2 = aliased(AssignedResource)

    states = ('toLaunch', 'toError', 'toAckReservation', 'Launching', 'Running ', 'Finishing')

    result = db.query(distinct(j2.id))\
               .filter(a1.index == 'CURRENT')\
               .filter(a2.index == 'CURRENT')\
               .filter(j1.id == job_id)\
               .filter(j1.id != j2.id)\
               .filter(a1.moldable_id == j1.assigned_moldable_job)\
               .filter(a2.resource_id == a1.resource_id)\
               .filter(j2.state.in_(states))\
               .all()

    return result


def resume_job_action(job_id):
    '''resume_job_action performs all action when a job is suspended'''

    set_job_state(job_id, 'Running')

    resources = get_current_resources_with_suspended_job()
    if resources != ():
        db.query(Resource)\
          .filter(~Resource.id.in_(resources))\
          .update({Resource.suspended_jobs: 'NO'}, synchronize_session=False)

    else:
        db.query(Resource)\
          .update({Resource.suspended_jobs: 'NO'}, synchronize_session=False)

    db.commit()


# get_cpuset_values_for_a_moldable_job
# get cpuset values for each nodes of a MJob
def get_cpuset_values(cpuset_field, moldable_id):
    # TODO TOFINISH
    logger.warning("get_cpuset_values is NOT ENTIRELY IMPLEMENTED")
    sql_where_string = "\'0\'"
    if "SCHEDULER_RESOURCES_ALWAYS_ASSIGNED_TYPE" in config:
        resources_to_always_add_type = config["SCHEDULER_RESOURCES_ALWAYS_ASSIGNED_TYPE"]
    else:
        resources_to_always_add_type = ""

    # TODO
    if resources_to_always_add_type != "":
        sql_where_string = "resources.type = \'$resources_to_always_add_type\'"

    results = db.query(Resource)\
                .filter(AssignedResource.moldable_id == moldable_id)\
                .filter(AssignedResource.resource_id == Resource.id)\


    # my $sth = $dbh->prepare("   SELECT resources.network_address, resources.$cpuset_field
    #                            FROM resources, assigned_resources
    #                            WHERE
    #                                assigned_resources.moldable_job_id = $moldable_job_id AND
    #                                assigned_resources.resource_id = resources.resource_id AND
    #                                resources.network_address != \'\' AND
    #                                (resources.type = \'default\' OR
    #                                 $sql_where_string)
    #                            GROUP BY resources.network_address, resources.$cpuset_field
    #                        ");

    return results
