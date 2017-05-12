# coding: utf-8
from __future__ import unicode_literals, print_function

from sqlalchemy import distinct
from oar.lib import (db, config, get_logger, AssignedResource, Resource,)
from oar.lib.event import (add_new_event, is_an_event_exists) 

logger = get_logger("oar.kao.tools")

# update_current_scheduler_priority
# Update the scheduler_priority field of the table resources
def update_current_scheduler_priority(job, value, state):
    """Update the scheduler_priority field of the table resources
    """

    # TO FINISH
    # TODO: MOVE TO resource.py ???

    logger.info("update_current_scheduler_priority " +
                " job.id: " + str(job.id) + ", state: " + state + ", value: "
                + str(value))

    if "SCHEDULER_PRIORITY_HIERARCHY_ORDER" in config:
        sched_priority = config["SCHEDULER_PRIORITY_HIERARCHY_ORDER"]
        if ((('besteffort' in job.types) or ('timesharing' in job.types)) and
           (((state == 'START') and
             is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") <= 0) or
           ((state == 'STOP') and is_an_event_exists(job.id, "SCHEDULER_PRIORITY_UPDATED_START") > 0))):

            coeff = 1
            if ('besteffort' in job.types) and ('timesharing' not in job.types):
                coeff = 10

            index = 0
            for f in sched_priority.split('/'):
                if f == '':
                    continue
                elif f == 'resource_id':
                    f = 'id'

                index += 1

                res = db.query(distinct(getattr(Resource, f)))\
                        .filter(AssignedResource.index == 'CURRENT')\
                        .filter(AssignedResource.moldable_id == job.assigned_moldable_job)\
                        .filter(AssignedResource.resource_id == Resource.id)\
                        .all()

                resources = tuple(r[0] for r in res)

                if resources == ():
                    return

                incr_priority = int(value) * index * coeff
                db.query(Resource)\
                  .filter((getattr(Resource, f)).in_(resources))\
                  .update({Resource.scheduler_priority: incr_priority}, synchronize_session=False)

            add_new_event('SCHEDULER_PRIORITY_UPDATED_' + state, job.id,
                          'Scheduler priority for job ' + str(job.id) +
                          'updated (' + sched_priority + ')')


def update_scheduler_last_job_date(date, moldable_id):
    db.query(Resource).filter(AssignedResource.Moldable_job_id == moldable_id)\
                      .filter(AssignedResource.Resource_id == Resource.resource_id)\
                      .update({Resource.last_job_date: date})






def fork_and_feed_stdin(cmd, timeout_cmd, nodes):
    logger.error("OAR::Tools::fork_and_feed_stdin NOT YET IMPLEMENTED")
    return True

# TODO
def send_to_hulot(cmd, data):
    config.setdefault_config({"FIFO_HULOT": "/tmp/oar_hulot_pipe"})
    fifoname = config["FIFO_HULOT"]
    try:
        with open(fifoname, 'w') as fifo:
            fifo.write('HALT:%s\n' % data)
            fifo.flush()
    except IOError as e:
        e.strerror = 'Unable to communication with Hulot: %s (%s)' % fifoname % e.strerror
        logger.error(e.strerror)
        return 1
    return 0


def get_oar_pid_file_name(job_id):
    logger.error("get_oar_pid_file_name id not YET IMPLEMENTED")


def get_default_suspend_resume_file():
    logger.error("get_default_suspend_resume_file id not YET IMPLEMENTED")


def manage_remote_commands():
    logger.error("manage_remote_commands id not YET IMPLEMENTED")
